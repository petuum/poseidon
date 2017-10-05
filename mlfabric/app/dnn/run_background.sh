#!/bin/bash

source envs.sh
pdsh -R ssh -w node-[1-24] "mkdir -p /media/raajay/tmp"
MYTMPDIR="$(mktemp -d -u -p /media/raajay/tmp/ tmpdir.XXXXXX)"
echo $MYTMPDIR
pdsh -R ssh -w node-[1-24] "mkdir -p ${MYTMPDIR}"

# copy file
pdcp -R ssh -w node-[1-24] dstat.sh /media/raajay
pdcp -R ssh -w node-[1-24] iperf_server.sh /media/raajay
pdcp -R ssh -w node-[1-24] iperf_client.sh /media/raajay

# kill all iperf
pdsh -R ssh -w node-[1-24] "pkill -9 iperf" &> /dev/null
echo "KILLALL existing iperf"

# launch background traffic
./background.py 1 305 ${MYTMPDIR}

echo "Sleeping for a minute"
sleep 60

# kill all background
pdsh -R ssh -w node-[1-24] "pkill -9 iperf" &> /dev/null

#timestamp,source_address,source_port,destination_address,destination_port,interval,transferred_bytes,bits_per_second
