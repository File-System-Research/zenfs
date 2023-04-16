#!/bin/bash

if [ $# != 1 ]; then
    echo "Usage: $0 <nullb ID>"
    exit 1
fi

nid=$1

if [ ! -b "/dev/nullb$nid" ]; then
    echo "/dev/nullb$nid: No such device"
    exit 1
fi

sudo echo 0 > /sys/kernel/config/nullb/nullb$nid/power
sudo rmdir /sys/kernel/config/nullb/nullb$nid

echo "Destroyed /dev/nullb$nid"