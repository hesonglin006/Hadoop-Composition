#!/bin/bash
port1=27006
port2=27008
PID=$(netstat -aplnt | grep :$port1 | awk '{print $7}' | awk -F"/" '{print $1}');
echo $PID
if [ -n "$PID" ]; then
    kill -9 $PID;
fi
PID2=$(netstat -aplnt | grep :$port2 | awk '{print $7}' | awk -F"/" '{print $1}')
echo $PID2
if [ -n "$PID2" ]; then
    kill -9 $PID2;
fi