#!/bin/bash

export DEBIAN_FRONTEND=noninteractive

echo $1 | sudo -S sed -i s/gr.ar/gb.ar/ /etc/apt/sources.list
echo $1 | sudo -S apt-get update || true
echo $1 | sudo -S apt-get -y install cifs-utils
echo $1 | sudo -S umount /mnt/nfs || true
echo $1 | sudo -S mkdir -p /mnt/nfs || true
echo $1 | sudo -S mount -t cifs //192.168.0.1/celar /mnt/nfs -o passwd=

