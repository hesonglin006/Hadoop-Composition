#!/bin/bash
port=47006
PID=$(netstat -aplnt | grep :$port | awk '{print $7}' | awk -F"/" '{print $1}');
echo $PID
if [ -n "$PID" ]; then
    kill -9 $PID;
fi
