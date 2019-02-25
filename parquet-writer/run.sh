#!/usr/bin/env bash
COUNT=0

while true
do
    COUNT=$((COUNT+1))
    curl http://192.168.99.103:9090/hello
    echo " ${COUNT}"
    sleep 1
done
