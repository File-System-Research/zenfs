#!/bin/bash

if [ $# != 4 ]; then
        echo "Usage: $0 <sect size (B)> <zone size (MB)> <nr conv zones> <nr seq zones>"
        exit 1
fi

scriptdir=$(cd $(dirname "$0") && pwd)

sudo modprobe null_blk nr_devices=0 || return $?

function create_zoned_nullb()
{
        local nid=0
        local bs=$1
        local zs=$2
        local nr_conv=$3
        local nr_seq=$4

        cap=$(( zs * (nr_conv + nr_seq) ))

        while [ 1 ]; do
                if [ ! -b "/dev/nullb$nid" ]; then
                        break
                fi
                nid=$(( nid + 1 ))
        done

        dev="/sys/kernel/config/nullb/nullb$nid"
        sudo mkdir "$dev"

        sudo echo $bs > "$dev"/blocksize
        sudo echo 0 > "$dev"/completion_nsec
        sudo echo 0 > "$dev"/irqmode
        sudo echo 2 > "$dev"/queue_mode
        sudo echo 1024 > "$dev"/hw_queue_depth
        sudo echo 1 > "$dev"/memory_backed
        sudo echo 1 > "$dev"/zoned

        sudo echo $cap > "$dev"/size
        sudo echo $zs > "$dev"/zone_size
        sudo echo $nr_conv > "$dev"/zone_nr_conv

        sudo echo 1 > "$dev"/power

        sudo echo mq-deadline > /sys/block/nullb$nid/queue/scheduler

        echo "$nid"
}

nulldev=$(create_zoned_nullb $1 $2 $3 $4)
echo "Created /dev/nullb$nulldev"