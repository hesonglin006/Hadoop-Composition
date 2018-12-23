#!/bin/bash
./StopClient.sh
rm -rf ./Files/*
./createFiles.sh $1 $2
