#!/bin/bash

# Ready a worker, based on Ubuntu Server LTS / 14.04 Okeanos image:

apt-get -y install cifs-utils default-jre python python-dev python-pip
pip install cherrypy

SERVER_IP=$(ss-get orchestrator-okeanos:hostname)
CELAR_REPO=http://snf-175960.vm.okeanos.grnet.gr
JC_VERSION=LATEST
JC_ARTIFACT=JCatascopia-Agent
JC_GROUP=eu.celarcloud.cloud-ms
JC_TYPE=tar.gz
DISTRO=$(eval cat /etc/*release)

#download,install and start jcatascopia agent... 
URL="$CELAR_REPO/nexus/service/local/artifact/maven/redirect?r=snapshots&g=$JC_GROUP&a=$JC_ARTIFACT&v=$JC_VERSION&p=$JC_TYPE" 
wget -O JCatascopia-Agent.tar.gz $URL 
tar xvfz JCatascopia-Agent.tar.gz 
eval "sed -i 's/server_ip=.*/server_ip=$SERVER_IP/g' JCatascopia-Agent-*/JCatascopiaAgentDir/resources/agent.properties" 
cd JCatascopia-Agent-* 
./installer.sh 
cd .. 
/etc/init.d/JCatascopia-Agent restart 


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

echo $SCHED_ADDRESS > ~/scan_sched_address

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

