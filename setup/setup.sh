#!/bin/bash

conda create -n flink python=3.8
conda activate flink

python -m pip install apache-flink
pip install pyflink

export FLINK_CONDA_HOME=$(dirname $(dirname $CONDA_EXE))