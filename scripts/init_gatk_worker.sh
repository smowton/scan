#!/bin/bash
# Ready a worker, based on Ubuntu Server LTS / 14.04 Okeanos image:

apt-get -y install cifs-utils

#
# update system
#
apt-get -o DPkg::options::="--force-confdef" -o DPkg::options::="--force-confold" -y upgrade
mkdir /mnt/nfs

# Wait for the scheduler to start CIFS server (nfs name is historical):
RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
while [ $RDY1 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
done

mount -t cifs //`ss-get --timeout 3600 scheduler.1:hostname`/share /mnt/nfs -o username=guest,password=''

apt-get -y install default-jre python

# Fetch the GATK:
mkdir ~/gatk
cd ~/gatk
wget http://cs448.user.srcf.net/GenomeAnalysisTK.jar

# Fetch the SCAN ps agent:
cd ~
git clone https://github.com/smowton/scan.git

# Fetch JCatascopia standard probes, etc.
# Not implemented yet
# Wait for the scheduler:
RDY2=`ss-get --timeout 3600 scheduler.1:sched_ready`
while [ $RDY2 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY2=`ss-get --timeout 3600 scheduler.1:sched_ready`
done

# Enable passwordless SSH access (for example?)
mkdir ~/.ssh

# auth_keys might not end with a newline at the moment
echo >> ~/.ssh/authorized_keys
echo `ss-get --timeout 3600 scheduler.1:authorized_keys | base64 -d` >> ~/.ssh/authorized_keys

# Machine is now ready to be a GATK worker. Register it:
SCHED_ADDRESS=`ss-get --timeout 3600 scheduler.1:sched_address`
# This might be tricky: discover my own class. The orchestrator knew this, and somehow needs to get that information through to the Slipstream phase.
WORKER_CLASS=`ss-get --timeout 3600 nodename`

~/scan/register_worker.py $SCHED_ADDRESS $WORKER_CLASS > ~/scan_worker_id
