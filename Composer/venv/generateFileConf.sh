#!/bin/bash
# Generate conf file for scheduling and composition
function rand(){
  min=$1
  max=$(($2-$min+1))
  num=$(($RANDOM+1000000000))
  echo $(($num%$max+$min))
}
rnd=$(rand 1 20)
#echo $rnd
if [ -f "conf.txt" ]; then
    rm -rf "conf.txt"
fi
# vp0
for ((i=1;i<=$1;i++));
do
echo 'vp0:104.196.200.233,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp1
for ((i=1;i<=$2;i++));
do
echo 'vp1:35.237.39.136,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp2
for ((i=1;i<=$3;i++));
do
echo 'vp2:35.237.228.117,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp3
for ((i=1;i<=$4;i++));
do
echo 'vp3:35.245.135.135,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp4
for ((i=1;i<=$5;i++));
do
echo 'vp4:35.236.192.229,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp5
for ((i=1;i<=$6;i++));
do
echo 'vp5:35.245.86.225,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp6
for ((i=1;i<=$7;i++));
do
echo 'vp6:35.238.222.230,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp7
for ((i=1;i<=$8;i++));
do
echo 'vp7:35.184.108.13,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
# vp8
for ((i=1;i<=$9;i++));
do
echo 'vp8:104.197.235.29,64,'$(rand 1 20)',data0'${i} >> ./conf.txt
done
