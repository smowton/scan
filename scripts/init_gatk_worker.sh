#!/bin/bash

# Ready a worker, based on Ubuntu Server LTS / 14.04 Okeanos or Flexiant image:

# Create unpriv user if needed
if ! getent passwd user > /dev/null; then
    adduser --disabled-password --gecos "" user
fi

# Install various dependencies:

apt-get update
apt-get -y install cifs-utils default-jre python python-dev python-pip libz-dev liblapack-dev libblas-dev cmake libjansi-java git python-h5py python-zmq python-matplotlib cython openjdk-7-jdk python-wxgtk2.8 python-scipy python-mysqldb python-vigra imagemagick nfs-common
pip install cherrypy

# Install GROMACS:
cd /home/user
wget http://cs448.user.srcf.net/gmxdist.tar.gz
tar xvzf gmxdist.tar.gz
cd gromacs-5.0.1/build
make install

# Install CellProfiler:
cd /home/user
wget http://cs448.user.srcf.net/cpdist.tar.gz
tar xvzf cpdist.tar.gz
cd CellProfiler

JAVABINPATH=`which java`
ABSJAVAPATH=`readlink -f $JAVABINPATH`
export JAVA_HOME=`dirname $ABSJAVAPATH`/../..
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$LD_LIBRARY_PATH

cd ~

# Set up JCatascopia

CLOUDSERVICE=$(ss-get scheduler.1:cloudservice)

if [ "$CLOUDSERVICE" = "okeanos" ]; then
    ORCHNAME="orchestrator-okeanos"
elif [ "$CLOUDSERVICE" = "Flexiant-c2" ]; then
    ORCHNAME="orchestrator-Flexiant-c2"
else
    echo "Unknown cloud service $CLOUDSERVICE"; exit 1
fi

SERVER_IP=$(ss-get $ORCHNAME:hostname)
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

# Install Scala:
cd /tmp
wget http://scala-lang.org/files/archive/scala-2.10.2.deb
dpkg -i scala-2.10.2.deb

mkdir /mnt/nfs

# Wait for the scheduler to start NFS server
RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
while [ $RDY1 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
done

mount `ss-get --timeout 3600 scheduler.1:hostname`:/mnt/nfs /mnt/nfs

# Fetch SCAN:
cd ~
git clone https://github.com/smowton/scan.git

# Build json.org.
cd /tmp
git clone https://github.com/douglascrockford/JSON-java.git
mkdir -p org/json
mv JSON-java/* org/json/
javac org/json/*.java
jar cvf ~/scan/json-org.jar org

# Build JobRunner (must happen after the sched starts, as it downloads Queue)
cd ~/scan/queue_jobrunner
scalac -cp /mnt/nfs/Queue-3.1-smowton.jar:/root/scan/json-org.jar *.scala

# For compatibility with the test environment...
cp -r ~/scan /home/user/scan

# Wait for the scheduler:
RDY2=`ss-get --timeout 3600 scheduler.1:sched_ready`
while [ $RDY2 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY2=`ss-get --timeout 3600 scheduler.1:sched_ready`
done

# Enable passwordless SSH access
mkdir ~/.ssh

# auth_keys might not end with a newline at the moment
echo >> ~/.ssh/authorized_keys
echo `ss-get --timeout 3600 scheduler.1:authorized_keys | base64 -d` >> ~/.ssh/authorized_keys
mkdir -p ~user/.ssh
echo `ss-get --timeout 3600 scheduler.1:authorized_keys | base64 -d` >> /home/user/.ssh/authorized_keys
chmod 600 ~user/.ssh/authorized_keys
chown -R user:user /home/user/.ssh

SCHED_ADDRESS=`ss-get --timeout 3600 scheduler.1:sched_address`

echo $SCHED_ADDRESS > ~/scan_sched_address

# Initialise SCANFS here
python ~/scan/btrfs_scripts/add-root-device.py
chown user:user /mnt/scanfs

# Machine is now ready to be a GATK worker. Register it:
~/scan/register_worker.py $SCHED_ADDRESS > ~/scan_worker_id

WORKER_ID=`cat ~/scan_worker_id`

# Build and configure probe:
~/scan/scripts/build_probes.sh

cd ~/scan/jc_probes
echo "/usr/local/bin/JCatascopiaAgentDir" > /etc/scan_probe
echo "probes_external=ScanWorkerProbe,`pwd`/ScanProbe.jar" >> /usr/local/bin/JCatascopiaAgentDir/resources/agent.properties
echo "host=$SCHED_ADDRESS" >> /usr/local/bin/JCatascopiaAgentDir/resources/scanprobe.properties
echo "workerid=$WORKER_ID" >> /usr/local/bin/JCatascopiaAgentDir/resources/scanprobe.properties
service JCatascopia-Agent stop
service JCatascopia-Agent start

