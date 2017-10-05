#!/bin/bash

source envs.sh
DURATION=5400
BCK_DURATION=$(( $DURATION + 100 ))

array=(2 4 7)

for i in "${array[@]}"
do

NUM_BCK=$i

# manage temp files
pdsh -R ssh -w node-[1-24] "mkdir -p /media/raajay/tmp"
MYTMPDIR="$(mktemp -d -u -p /media/raajay/tmp/ tmpdir.XXXXXX)"
echo $MYTMPDIR

pdsh -R ssh -w node-[1-24] "mkdir -p ${MYTMPDIR}" &> /dev/null
echo "CREATE remote tmp dirs"

# copy file
pdcp -R ssh -w node-[1-24] dstat.sh /media/raajay &> /dev/null
pdcp -R ssh -w node-[1-24] iperf_server.sh /media/raajay &> /dev/null
pdcp -R ssh -w node-[1-24] iperf_client.sh /media/raajay &> /dev/null
pdcp -R ssh -w node-[1-24] sysbench.sh /media/raajay &> /dev/null
echo "COPY required scripts"

# kill all iperf
pdsh -R ssh -w node-[1-24] "pkill -9 iperf" &> /dev/null
echo "KILLALL existing iperf"

if [ $NUM_BCK -gt 0 ]
then
# launch background traffic
./background.py ${NUM_BCK} ${BCK_DURATION} ${MYTMPDIR}
echo "LAUNCH ALL background network transfers"

# launch background compute jobs
NUM_COMPUTE_START=3
NUM_COMPUTE_END=$(( NUM_BCK * 2 + 2  ))
pdsh -R ssh -w node-[$NUM_COMPUTE_START-$NUM_COMPUTE_END] "nohup sysbench --test=cpu --num-threads=8 --cpu-max-prime=2000000 --max-time=$DURATION run &> /dev/null &"
echo "LAUNCH ALL background network jobs"
fi


# kill all dstat
pdsh -R ssh -w node-[1-24] "pkill dstat" &> /dev/null
pdsh -R ssh -w node-[1-24] "rm ${DSTAT_LOG_FILE}" &> /dev/null
echo "KILL DSTAT in ALL"


# launch dstat
pdsh -R ssh -w node-[1-24] "/media/raajay/dstat.sh ${DSTAT_LOG_FILE}" &> /dev/null
echo "LAUNCH DSTAT in ALL"


./script/launch.py
sleep ${DURATION}
./script/kill.py ${BOSEN_CONFIG_FILE}


# kill all dstat
pdsh -R ssh -w node-[1-24] "pkill dstat" &> /dev/null

# kill all background
pdsh -R ssh -w node-[1-24] "pkill -9 iperf" &> /dev/null

# kill all sysbench
pdsh -R ssh -w node-[3-12] "ps aux | grep sysbench"
pdsh -R ssh -w node-[3-12] "pkill sysbench"


# pull logs
mkdir -p ./results/${EXPERIMENT_NAME}-LOW${NUM_BCK}
mkdir -p ./results/${EXPERIMENT_NAME}-LOW${NUM_BCK}/iperf

rpdcp -R ssh -w node-[1-24] "/media/raajay/ps/logs/${EXPERIMENT_NAME}/*" ./results/${EXPERIMENT_NAME}-LOW${NUM_BCK}/
# some of the hosts do not have bck log
rpdcp -R ssh -w node-[1-24] "${MYTMPDIR}/*" ./results/${EXPERIMENT_NAME}-LOW${NUM_BCK}/iperf &> /dev/null


#pdsh -R ssh -w node-[1-24] "nohup dstat -d -c -n -N total --output /media/raajay/ps/logs/dev-large-single/dstat.log 2>&1 > /dev/null"
#pdsh -R exec -f $THREADS -w ^instances ssh -o ConnectTimeout=$TIMEOUT %h '( . /users/rgrandl/run.sh -q ; hadoop-daemon.sh start datanode;)'
#pdsh -R exec -w node-[1-24] ssh -o ConnectTimeout=1 %h '( . /media/raajay/dstat.sh ${DSTAT_LOG_FILE}; )'

echo
echo
echo
echo "FINISHED experiment:", $NUM_BCK
echo
echo
echo

done
