#!/usr/bin/env python

import os
from os.path import dirname, join
import time
import sys

#hostfile_name = "machinefiles/localserver"

#app_dir = dirname(dirname(os.path.realpath(__file__)))
app_dir = os.environ.get('DNN_APP_DIRECTORY', "/media/raajay/ps/bosen/app/dnn")
proj_dir = dirname(dirname(app_dir))

#hostfile = join(proj_dir, hostfile_name)
hostfile = os.environ.get('BOSEN_CONFIG_FILE',
                          join(proj_dir, "machinefiles/localserver"))

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    #"-o UserKnownHostsFile=/dev/null "
    )

# Get client: host IPs
with open(hostfile, "r") as f:
  hostlines = f.read().splitlines()
  host_ips = {}
  for line in hostlines:
    parts = line.strip().split()
    if len(parts) != 3:
      print "wrong format for host file"
      sys.exit()
    host_ips[int(parts[0])] = parts[1]
    # host_ips = [line.split()[1] for line in hostlines]

# env variables required by run_local.py
env_variables = [
    "GLOG_VERBOSITY",
    "BOSEN_CONFIG_FILE",
    "BOSEN_NUM_THREADS",
    "BOSEN_NUM_COMM_CLIENTS",
    "STALENESS",
    "DNN_APP_DIRECTORY",
    "DNN_PARAMETER_FILE",
    "DNN_DATA_PARTITION_FILE",
    "DNN_STATISTICS_FILE",
    "REMOTE_LOG_DIRECTORY"
]

env_params = ""
for var in env_variables:
    value = os.environ.get(var)
    if value is None:
        continue
    # notice the space at the end
    env_params += "%s=%s " % (var, value)

fp = open("./cmds.log", "w")
# for client_id, ip in enumerate(host_ips):
for client_id, ip in host_ips.iteritems():
  cmd = ssh_cmd + ip + " "
  cmd += "\'" + env_params + "python " + join(app_dir, "script/run_local.py")
  cmd += " %d %s\'" % (client_id, hostfile)
  cmd += " &"
  # print cmd
  fp.write(cmd+"\n")
  os.system(cmd)

  if client_id == 0:
    print "Waiting for first client to set up"
    time.sleep(2)
fp.close()
print "Done launching all the jobs. See cmds.log for all ssh commands used."
