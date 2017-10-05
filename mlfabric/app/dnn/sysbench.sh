#!/bin/bash
echo "Launch sysbench for "$1
sysbench --test=cpu --cpu-max-prime=2000000 --num-threads=16 --max-time $1 run
