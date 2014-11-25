#!/bin/bash
# Ready a worker, based on Ubuntu Server LTS / 14.04 Okeanos image:

apt-get -y install cifs-utils default-jre python python-dev python-pip
pip install cherrypy

#
# update system
#
#apt-get -o DPkg::options::="--force-confdef" -o DPkg::options::="--force-confold" -y upgrade
mkdir /mnt/nfs

# Wait for the scheduler to start CIFS server (nfs name is historical):
RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
while [ $RDY1 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
done

mount -t cifs //`ss-get --timeout 3600 scheduler.1:hostname`/share /mnt/nfs -o username=guest,password=''

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

SCHED_ADDRESS=`ss-get --timeout 3600 scheduler.1:sched_address`
# This might be tricky: discover my own class. The orchestrator knew this, and somehow needs to get that information through to the Slipstream phase.
WORKER_CLASS=`ss-get --timeout 3600 nodename`

echo $SCHED_ADDRESS > ~/scan_sched_address
echo $WORKER_CLASS > ~/scan_worker_class

# Machine is now ready to be a GATK worker. Register it:
~/scan/register_worker.py $SCHED_ADDRESS $WORKER_CLASS > ~/scan_worker_id

WORKER_ID=`cat ~/scan_worker_id`

# Build and configure probe:
~/scan/scripts/build_probes.sh

cd ~/scan/jc_probes
echo "/usr/local/bin/JCatascopiaAgentDir" > /etc/scan_probe
echo "probes_external=ScanWorkerProbe,`pwd`/ScanProbe.jar" >> /usr/local/bin/JCatascopiaAgentDir/resources/agent.properties
echo "host=$SCHED_ADDRESS" >> /usr/local/bin/JCatascopiaAgentDir/resources/scanprobe.properties
echo "class=$WORKER_CLASS" >> /usr/local/bin/JCatascopiaAgentDir/resources/scanprobe.properties
echo "workerid=$WORKER_ID" >> /usr/local/bin/JCatascopiaAgentDir/resources/scanprobe.properties
service JCatascopia-Agent stop
service JCatascopia-Agent start

