#!/usr/bin/env python

"""
This script starts a process locally, using <client-id> <hostfile> as inputs.
"""

import os
from os.path import dirname
from os.path import join
import time
import sys

if len(sys.argv) != 3:
  print "usage: %s <client-id> <hostfile>" % sys.argv[0]
  sys.exit(1)

# Please set the FULL app dir path here
#app_dir = "/home/user/bosen/app/dnn"
app_dir = os.environ.get('DNN_APP_DIRECTORY', "/media/raajay/ps/bosen/app/dnn")

client_id = sys.argv[1]
hostfile = sys.argv[2]

# go up two directories from app_dir
proj_dir = dirname(dirname(app_dir))

params = {
    "staleness": os.environ.get('STALENESS', '5')
#     , "parafile": os.environ.get('DNN_PARAMETER_FILE' ,join(app_dir, "datasets/para_imnet.txt"))
     , "parafile": os.environ.get('DNN_PARAMETER_FILE', '/media/raajay/ps/configs/dnn_param_file')
#     , "parafile": join(app_dir, "datasets/para_imnet.txt")
    #, "parafile": "hdfs://hdfs-domain/user/bosen/dataset/dnn/datasets/para_imnet.txt"

     , "data_ptt_file": os.environ.get('DNN_DATA_PARTITION_FILE' ,join(app_dir, "datasets/data_ptt_file.txt"))
#     , "data_ptt_file": join(app_dir, "datasets/data_ptt_file.txt")
    #, "data_ptt_file": "hdfs://hdfs-domain/user/bosen/dataset/dnn/datasets/data_ptt_file.txt"

     , "model_weight_file": join(app_dir, "datasets/weights.txt")
    #, "model_weight_file": "hdfs://hdfs-domain/user/bosen/dataset/dnn/datasets/weights.txt"

     , "model_bias_file": join(app_dir, "datasets/biases.txt")
    #, "model_bias_file": "hdfs://hdfs-domain/user/bosen/dataset/dnn/datasets/biases.txt"

    , "stats_path": os.environ.get('DNN_STATISTICS_FILE', '/scratch/raajay/ps/logs/dnn-bosen-stats.log')
    }


petuum_params = {
    "hostfile": hostfile,
    "num_worker_threads": int(os.environ.get('BOSEN_NUM_THREADS', '32')),
    "num_comm_clients": int(os.environ.get('BOSEN_NUM_COMM_CLIENTS', '32'))
    }


#build_dir = join(proj_dir, "build", "app", "dnn")
#prog_name = "dnn_main"
#build_dir = "build"
prog_name = "DNN"
app_name = app_dir.split('/')[-1]

#prog_path = os.path.join(proj_dir, build_dir, "app", app_name, prog_name)
prog_path = os.path.join(app_dir, "bin", prog_name)


# hadoop_path = os.popen('hadoop classpath --glob').read()

env_params = "".join((
  "GLOG_logtostderr=true ",
  "GLOG_v=%s " % os.environ.get('GLOG_VERBOSITY', '0'),
  "GLOG_minloglevel=0 "
  ))

# Get host IPs
with open(hostfile, "r") as f:
  hostlines = f.read().splitlines()
host_ips = [line.split()[1] for line in hostlines]
petuum_params["num_clients"] = len(host_ips)


# cmd = "killall -q " + prog_name
# os.system is synchronous call.
# os.system(cmd)
# print "Done killing"

# cmd = "export CLASSPATH=`hadoop classpath --glob`:$CLASSPATH; "
cmd = " "
cmd += env_params + prog_path
petuum_params["client_id"] = client_id
cmd += "".join([" --%s=%s" % (k,v) for k,v in petuum_params.items()])
cmd += "".join([" --%s=%s" % (k,v) for k,v in params.items()])
log_dir = os.environ.get('REMOTE_LOG_DIRECTORY', '/media/raajay/ps/logs')
cmd += " 1>%s/dnn-stdout-%s.log 2>%s/dnn-stderr-%s.log" % (log_dir, client_id, log_dir, client_id)
#print cmd
os.system(cmd)
