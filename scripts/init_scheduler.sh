#!/bin/bash 

ip=$(ss-get hostname) 
hostname=$(hostname) 
echo $ip $hostname >> /etc/hosts 

# Install various dependencies...

apt-get update -y 
apt-get install -y openjdk-7-jdk openjdk-7-jre-headless nfs-kernel-server python python-pip python-dev git pkg-config libfreetype6-dev python-numpy python-matplotlib 
pip install cql cherrypy

# Set up JCatascopia

SERVER_IP=$(ss-get orchestrator-okeanos:hostname) 
CELAR_REPO=http://snf-175960.vm.okeanos.grnet.gr 
JC_VERSION=LATEST 
JC_ARTIFACT=JCatascopia-Agent 
JC_GROUP=eu.celarcloud.cloud-ms 
JC_TYPE=tar.gz 
DISTRO=$(eval cat /etc/*release) 

URL="$CELAR_REPO/nexus/service/local/artifact/maven/redirect?r=snapshots&g=$JC_GROUP&a=$JC_ARTIFACT&v=$JC_VERSION&p=$JC_TYPE" 
wget -O JCatascopia-Agent.tar.gz $URL 
tar xvfz JCatascopia-Agent.tar.gz 
eval "sed -i 's/server_ip=.*/server_ip=$SERVER_IP/g' JCatascopia-Agent-*/JCatascopiaAgentDir/resources/agent.properties" 
cd JCatascopia-Agent-* 
./installer.sh 
cd .. 
/etc/init.d/JCatascopia-Agent restart 

# Set up NFS share

mkdir -p /mnt/nfs
chown user:user /mnt/nfs

echo "/mnt/nfs 83.212.0.0/16(rw,sync,no_subtree_check) 2001:648:2ffc:1225::/64(rw,sync,no_subtree_check)" >> /etc/exports
service nfs-kernel-server restart

touch /mnt/nfs/test.file

ss-set nfs_ready 1

# Get the scheduler code etc:

cd ~
git clone https://github.com/smowton/scan.git

# Create SSH keys and publish the public key for workers to use:
mkdir -p ~/.ssh
ssh-keygen -N "" -f ~/.ssh/id_rsa
ss-set authorized_keys `cat ~/.ssh/id_rsa.pub | base64 --wrap 0`

# Register probe with JC agent:
~/scan/scripts/build_probes.sh

cd ~/scan/jc_probes
echo "probes_external=ScanProbe,`pwd`/ScanProbe.jar" >> /usr/local/bin/JCatascopiaAgentDir/resources/agent.properties

# Fetch Queue:
cd /mnt/nfs
wget http://cs448.user.srcf.net/Queue-3.1-smowton.jar

# Start the scheduler
cd ~/scan
~/scan/tinysched.py ~/scan/queue_scripts/gatk_pipeline_classes.py &
~/scan/await_server.py

# Reload to enable the SCAN probe
service JCatascopia-Agent stop
service JCatascopia-Agent start

# Note that we're ready
ss-set sched_address `ss-get hostname`
ss-set sched_ready 1


