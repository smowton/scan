#!/bin/bash

#
# update system
#
apt-get -o DPkg::options::="--force-confdef" -o DPkg::options::="--force-confold" -y upgrade
mkdir /mnt/nfs

apt-get -y install cifs-utils default-jre python default-jdk libjansi-java

# Install Scala:
cd /tmp
http://scala-lang.org/files/archive/scala-2.10.2.deb
dpkg -i scala-2.10.2.deb

# Fetch JSON.org:
cd ~/scan
wget "http://search.maven.org/remotecontent?filepath=org/codeartisans/org.json/20131017/org.json-20131017.jar" -O json-org.jar

# Wait for the scheduler to start CIFS server (nfs name is historical):
RDY1=`ss-get scheduler.1:nfs_ready`
while [ $RDY1 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY1=`ss-get scheduler.1:nfs_ready`
done

mount -t cifs //`ss-get --timeout 480 scheduler.1:hostname`/share /mnt/nfs -o username=guest,password=''

# Fetch Queue:
cd /mnt/nfs
wget http://cs448.user.srcf.net/Queue-3.1-smowton.jar

# Build JobRunner
cd ~/scan/queue_jobrunner
scalac -cp /mnt/nfs/Queue-3.1-smowton.jar:~/scan/json-org.jar *.scala

# Wait for scheduler:
RDY2=`ss-get scheduler.1:sched_ready`
while [ $RDY2 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY2=`ss-get scheduler.1:sched_ready`
done


