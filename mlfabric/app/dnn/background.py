#!/usr/bin/python
import os
import sys
import random

if (len(sys.argv) != 4):
    print "usage: ./script/background.py <num_comms> <duration> <tmpdir>"
    sys.exit()

busy_hosts = ["node-%d" % x for x in range(3, 21)]
num_comms = int(sys.argv[1])
duration = int(sys.argv[2])
tmpdir = sys.argv[3]

client_counter = dict()
server_counter = dict()


curr_server = 3
curr_client = 4


for h in ["node-%d" % x for x in range(3, 24)]:
    client_counter[h] = 1
    server_counter[h] = 1


ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
#    "-o UserKnownHostsFile=/dev/null "
    )


def pick_random_pair():
    if(len(busy_hosts) == 1):
        print "only one host avl."
        return

    src = random.choice(busy_hosts)
    dst = random.choice(busy_hosts)
    while src == dst:
        dst = random.choice(busy_hosts)
        pass
    return (src, dst)


def pick_debug_pair():
    return ("node-2", "node-3")


def pick_next_pair():
    global curr_server
    global curr_client
    server = "node-%d" % curr_server
    client = "node-%d" % curr_client
    curr_server += 2
    curr_client += 2
    return (server, client)


def pick_src_dst_pairs(count):
    return [pick_next_pair() for x in range(0, count)]


def launch_server(host):
    iperf_log_file = os.path.join(tmpdir, "iperf-server.log." + str(server_counter[host]))
    cmd = ssh_cmd + host + " "
    cmd += "\'/media/raajay/iperf_server.sh %s\'" % iperf_log_file
    #print cmd
    os.system(cmd)
    server_counter[host] += 1


def launch_client(host, server):
    iperf_log_file = os.path.join(tmpdir, "iperf-client.log." + str(client_counter[host]))
    cmd = ssh_cmd + host + " "
    cmd += "\'/media/raajay/iperf_client.sh %s %d %s\'" % (server, duration, iperf_log_file)
    print host, " to ", server
    os.system(cmd)
    client_counter[host] += 1
    print client_counter[host]


servers = set()
for x in pick_src_dst_pairs(num_comms):
    server = x[0]
    client = x[1]
    if server not in servers:
        launch_server(server)
        servers.add(server)
    launch_client(client, server)
