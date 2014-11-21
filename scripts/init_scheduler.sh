#!/bin/bash 

ip=$(ss-get hostname) 
hostname=$(hostname) 
echo $ip $hostname >> /etc/hosts 

SERVER_IP=$(ss-get orchestrator-Flexiant:hostname) 
CELAR_REPO=http://snf-175960.vm.okeanos.grnet.gr 
JC_VERSION=LATEST 
JC_ARTIFACT=JCatascopia-Agent 
JC_GROUP=eu.celarcloud.cloud-ms 
JC_TYPE=tar.gz 
DISTRO=$(eval cat /etc/*release) 

if [[ "$DISTRO" == *Ubuntu* ]]; then 
        apt-get update -y 
        #download and install java 
        apt-get install -y openjdk-7-jre-headless 
fi 
if [[ "$DISTRO" == *CentOS* ]]; then 
        yum -y update 
        yum install -y wget 
        #download and install java 
        yum -y install java-1.7.0-openjdk 
fi 

#download,install and start jcatascopia agent... 
URL="$CELAR_REPO/nexus/service/local/artifact/maven/redirect?r=snapshots&g=$JC_GROUP&a=$JC_ARTIFACT&v=$JC_VERSION&p=$JC_TYPE" 
wget -O JCatascopia-Agent.tar.gz $URL 
tar xvfz JCatascopia-Agent.tar.gz 
eval "sed -i 's/server_ip=.*/server_ip=$SERVER_IP/g' JCatascopia-Agent-*/JCatascopiaAgentDir/resources/agent.properties" 
cd JCatascopia-Agent-* 
./installer.sh 
cd .. 
/etc/init.d/JCatascopia-Agent restart 

# Set up Samba and Java
apt-get -y install samba openjdk-7-jdk
# Upgrade everything
apt-get -o DPkg::options::="--force-confdef" -o DPkg::options::="--force-confold" -y upgrade

sed 's/WORKGROUP/SCAN/' < /etc/samba/smb.conf > /tmp/smb.conf ; mv -f /tmp/smb.conf /etc/samba/smb.conf
echo 'security = share' >> /etc/samba/smb.conf
echo '[share]' >> /etc/samba/smb.conf
echo '   comment = Ubuntu File Server Share' >> /etc/samba/smb.conf
echo '   path = /mnt/nfs' >> /etc/samba/smb.conf
echo '   public = yes' >> /etc/samba/smb.conf
echo '   guest ok = yes' >> /etc/samba/smb.conf
echo '   guest only = yes' >> /etc/samba/smb.conf
echo '   guest account = nobody' >> /etc/samba/smb.conf
echo '   browsable = yes' >> /etc/samba/smb.conf
echo '   read only = no' >> /etc/samba/smb.conf

mkdir -p /mnt/nfs
chown nobody:nogroup /mnt/nfs

restart smbd
restart nmbd

#create a test.file
touch /mnt/nfs/test.file

ss-set nfs_ready 1

apt-get -y install python python-pip python-dev git pkg-config libfreetype6-dev python-numpy python-matplotlib
pip install cql cherrypy

# Get the scheduler code etc:
cd ~
git clone https://github.com/smowton/scan.git

# Create SSH keys and publish the public key for workers to use:
mkdir -p ~/.ssh
ssh-keygen -N "" -f ~/.ssh/id_rsa
ss-set authorized_keys `cat ~/.ssh/id_rsa.pub | base64 --wrap 0`

# Wait for Cassandra to come up
RDY1=`ss-get cassandraSeedNode.1:cassandraReady`
while [ $RDY1 != "true" ]; do
    echo "Waiting for Cassandra..."
    sleep 1
    RDY1=`ss-get cassandraSeedNode.1:cassandraReady`
done

# Export for the scheduler's benefit
export SCAN_DB_HOST=`ss-get cassandraSeedNode.1:hostname`
# Store for work-generator clients to see and use
echo $SCAN_DB_HOST > /mnt/nfs/scan_db_hostname

# Initialise DB:
~/scan/integ_analysis/initdb.py $SCAN_DB_HOST

# Set up test environment:
# Make sure test_workdir is usable by remote users
sudo -u nobody mkdir /mnt/nfs/test_workdir
mkdir /mnt/nfs/test_input
cd /mnt/nfs/test_input
wget http://cs448.user.srcf.net/in.bam
wget http://cs448.user.srcf.net/in.bam.bai

# Register probe with JC agent:
~/scan/scripts/build_probes.sh

cd ~/scan/jc_probes
echo "probes_external=ScanProbe,`pwd`/ScanProbe.jar" >> /usr/local/bin/JCatascopiaAgentDir/resources/agent.properties
service JCatascopia-Agent stop
service JCatascopia-Agent start

# Start the scheduler
~/scan/tinysched.py ~/scan/queue_scripts/gatk_pipeline_classes.py gatk_ssh_username=root &
~/scan/await_server.py

# Note that we're ready
ss-set sched_address `ss-get hostname`

ss-set sched_ready 1


