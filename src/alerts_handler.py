"""REST API alert handler for the Flink consumer."""

import aiohttp
import asyncio
import io
import threading
import unittest.mock as mock
from typing import Dict, List

import fastavro

from logger import Logger

ALERTS_BUFFER = []
ALERTS_BUFFER_LOCK = threading.Lock()
API_URL = "http://example.com/api/alert"
LOGGER = Logger("alert")
SCHEMA = {
    "type": "record",
    "name": "AlertData",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "year", "type": "string"},
        {"name": "make", "type": "string"},
        {"name": "model", "type": "string"},
        {"name": "price", "type": "double"},
    ],
}


async def send_alerts(alerts: List[Dict]) -> None:
    """
    Sends alerts to the API using aiohttp in batches.

    Parameters
    ----------
    alerts : list
        List of dictionaries containing the alerts to be sent.
    """
    try:
        ids = [alert["id"] for alert in alerts]
        ids = ', '.join(str(i) for i in ids)
        LOGGER.info(f"Sending {len(alerts)} total alerts...")

        async with aiohttp.ClientSession() as session:
            with mock.patch("aiohttp.ClientSession.post") as post_mock:
                post_mock.return_value.status = 200
                payload = serialize_alerts(alerts)

                async with session.post(API_URL, data=payload) as response:
                    response.raise_for_status()
                    LOGGER.info(
                        f"API POST request completed for ids: {ids}\n"
                    )
    except Exception as ex:
        LOGGER.error(f"Failed to send alerts: {ex}", exc_info=True)


def send_alerts_batch() -> None:
    """
    Sends all the alerts in the buffer to the API in batches.
    """
    global ALERTS_BUFFER
    with ALERTS_BUFFER_LOCK:
        alerts = ALERTS_BUFFER
        ALERTS_BUFFER = []
    asyncio.run(send_alerts(alerts))


async def buffer_alerts(alerts: List[Dict]) -> None:
    """
    Adds alerts to a buffer and sends them to the API in batches.

    Parameters
    ----------
    alerts : list
        List of dictionaries containing the alerts to be sent.
    """
    global ALERTS_BUFFER
    with ALERTS_BUFFER_LOCK:
        ALERTS_BUFFER.extend(alerts)
        if len(ALERTS_BUFFER) >= 10:
            threading.Thread(target=send_alerts_batch).start()


def serialize_alerts(alerts: List[Dict]) -> bytes:
    """
    Serializes alerts using Apache Avro.

    Parameters
    ----------
    alerts : list
        List of dictionaries containing the alerts to be serialized.

    Returns
    -------
    bytes
        Serialized alerts in bytes format.
    """
    buffer = io.BytesIO()
    fastavro.writer(buffer, SCHEMA, alerts)
    return buffer.getvalue()
