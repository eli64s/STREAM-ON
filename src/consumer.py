"""Data stream processing with Apache Flink and Python."""

import asyncio
import pandas as pd
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    CheckpointingMode,
    StreamExecutionEnvironment,
    TimeCharacteristic,
)
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.table import StreamTableEnvironment

from alerts_handler import buffer_alerts
from logger import Logger

LOGGER = Logger("alert")
SOURCE_DATA = pd.read_csv("data/data.csv")[
    [
        "id", "year_dt", "make", "model", "price"
        ]
    ]


def get_flink_environ():
    """
    1. StreamExecutionEnvironment.get_execution_environment(): creates new
    execution environment for Flink that manages the execution of streaming
    data processing jobs.
    """
    env = StreamExecutionEnvironment.get_execution_environment()

    """
    2. env.set_parallelism(1): sets the parallelism of the Flink job to 2,
    which means that all the processing of the streaming data will be done 
    with two threads.
    """
    env.set_parallelism(1)
    """
    3. env.set_stream_time_characteristic(TimeCharacteristic.EventTime): sets
    time characteristic of the streaming data to be event time, which means
    that the processing of events will be based on the timestamp of the events.
    """
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    """
    4. env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE):
    enables checkpointing for the Flink job every 10 seconds. Checkpointing
    is a mechanism for ensuring fault tolerance in Flink by periodically saving
    the state of the job. The CheckpointingMode.EXACTLY_ONCE ensures that each
    record is processed xactly once, even in the presence of failures.
    """
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    """
    5. env.set_buffer_timeout(5000): sets buffer timeout to 5 seconds. The
    Buffer timeout is the maximum amount of time that a record can be buffered
    before it is processed.
    """
    env.set_buffer_timeout(5000)
    """
    6. t_env = StreamTableEnvironment.create(env): StreamTableEnvironment
    provides a SQL-like interface for processing streaming data. It is used
    for defining tables, queries, and aggregations over streams.
    """
    t_env = StreamTableEnvironment.create(env)
    return (env, t_env)


async def create_source_table(t_env):
    """
    Create the source table in the Flink table environment.

    Parameters
    ----------
    t_env : StreamTableEnvironment
        The Flink table environment.
    """
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS source_data (
            id INT,
            year_dt AS CAST('2023' AS STRING),
            make AS CAST('Toyota' AS STRING),
            model AS CAST('Camry' AS STRING),
            price FLOAT
        ) WITH (
            'connector'='datagen',
            'rows-per-second'='20',
            'fields.id.min'='1',
            'fields.id.max'='999',
            'fields.price.min'='4000',
            'fields.price.max'='75000'
        )
    """
    )


async def create_batch_view(t_env):
    """
    Create the batch view in the Flink table environment.

    Parameters
    ----------
    t_env : StreamTableEnvironment
        The Flink table environment.
    """
    if "batch_data" not in t_env.list_tables():
        batch_data = t_env.from_pandas(SOURCE_DATA)
        t_env.create_temporary_view("batch_data", batch_data)
        LOGGER.info("Temporary view created for validation dataset.")
    else:
        LOGGER.info("Temporary view already exists for validation dataset.")


def process_flagged_record(record):
    """
    Process the flagged record and send an alert.

    Parameters
    ----------
    record : Tuple
        The flagged record as a tuple.
    """
    app_data = {
        "id": record[0],
        "year": record[1],
        "make": record[2],
        "model": record[3],
        "price": float(record[4]),
    }
    LOGGER.debug(f"Alert raised for application: {app_data}")
    return asyncio.run(buffer_alerts([app_data]))


async def main():
    """Orchestrates the stream processing engine."""
    LOGGER.info("Starting PyFlink stream processing engine...")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    env, t_env = get_flink_environ()
    await create_source_table(t_env)
    await create_batch_view(t_env)

    source_ds = t_env.to_append_stream(
        t_env.from_path("source_data"),
        Types.ROW(
            [
                Types.LONG(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.DOUBLE(),
            ]
        ),
    ).window_all(TumblingProcessingTimeWindows.of(Time.seconds(60)))

    batch_ds = t_env.to_append_stream(
        t_env.from_path("batch_data"),
        Types.ROW(
            [
                Types.LONG(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.DOUBLE(),
            ]
        ),
    ).window_all(TumblingProcessingTimeWindows.of(Time.seconds(60)))

    joined_table = t_env.sql_query(
        """
        SELECT src.*
        FROM source_data AS src
        WHERE src.id IN (
            SELECT bat.id
            FROM batch_data AS bat
            WHERE
                src.year_dt <> bat.year_dt
                or src.make <> bat.make
                or src.model <> bat.model
                or src.price <> bat.price
        )
    """
    )

    joined_ds = t_env.to_retract_stream(
        joined_table,
        Types.ROW_NAMED(
            ["id", "year_dt", "make", "model", "price"],
            [
                Types.LONG(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.DOUBLE(),
            ],
        ),
    )

    joined_ds.filter(lambda row: row[0]).map(lambda row: row[1]).map(
        process_flagged_record
    )

    joined_ds.print()

    env.execute("Flink data stream processor.")

    loop.run_until_complete(asyncio.gather(*asyncio.all_tasks()))

    LOGGER.info("Shutting down PyFlink stream processing engine...\n")


if __name__ == "__main__":
    asyncio.run(main())
