#!/bin/bash
nohup iperf -s -f Mbits -y c -i 1 -u &> $1 &
