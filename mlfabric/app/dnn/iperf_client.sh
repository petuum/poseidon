#!/bin/bash
echo "Connecting to " $1 "duration=" $2 "logfile=" $3
sleep 1
nohup iperf -c $1 -f Mbits -y c -t $2 -i 1 -d -u -b 6000000000 &> $3 &
