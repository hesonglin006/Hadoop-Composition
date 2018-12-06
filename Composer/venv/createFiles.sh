#!/bin/bash

node=$1
num=$2
#size=$3
#count=$4
cd ./Files/
for ((i=1;i<=$num;i++))
do
#if [ $num -ge 0 -a $num -lt 10 ]; then
dd if=/dev/zero of=vp${node}-data0${i}.csv bs=64M count=1;
#fi
#dd if=/dev/zero of=vp${node}-data
done