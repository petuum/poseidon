#!/bin/bash
echo $1
nohup dstat -c -d -n -N total --output $1 </dev/null &> /dev/null &
