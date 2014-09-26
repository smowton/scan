#!/bin/bash
# Ready a worker, based on Ubuntu Server LTS / 14.04 Okeanos image:

apt-get install default-jre git python

# Fetch the GATK:
mkdir ~/gatk
cd ~/gatk
wget http://cs448.user.srcf.net/GenomeAnalysisTK.jar

# Fetch the SCAN ps agent:
cd ~
git clone https://github.com/smowton/scan.git

# Fetch JCatascopia standard probes, etc.

# Wait for the scheduler:
RDY=`ss-get sched_ready`
while [ $RDY != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY=`ss-get sched_ready`
done

# Enable passwordless SSH access (for example?)
mkdir ~/.ssh
ss-get authorized-keys > ~/.ssh/authorized_keys

# Machine is now ready to be a GATK worker. Register it:
SCHED_ADDRESS=`ss-get sched_address`
# This might be tricky: discover my own class. The orchestrator knew this, and somehow needs to get that information through to the Slipstream phase.
WORKER_CLASS=`ss-get this_worker_class`
ME=`hostname -f`
~/scan/register_worker.py $SCHED_ADDRESS $WORKER_CLASS
