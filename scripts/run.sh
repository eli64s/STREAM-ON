#!/bin/bash

# Start Flink cluster
flink/bin/start-cluster.sh

# Submit PyFlink job
flink/bin/flink run -py ./scripts/word_count.py

# Stop Flink cluster
flink/bin/stop-cluster.sh