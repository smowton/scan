#!/bin/bash -x

#
# update system
#
#apt-get -o DPkg::options::="--force-confdef" -o DPkg::options::="--force-confold" -y upgrade
mkdir /mnt/nfs

apt-get -y install cifs-utils default-jre python default-jdk libjansi-java

# Install Scala:
cd /tmp
wget http://scala-lang.org/files/archive/scala-2.10.2.deb
dpkg -i scala-2.10.2.deb

# Build json.org. Still in /tmp:
git clone https://github.com/douglascrockford/JSON-java.git
mkdir -p org/json
mv JSON-java/* org/json/
javac org/json/*.java org/json/zip/*.java
jar cvf ~/scan/json-org.jar org

# Wait for the scheduler to start CIFS server (nfs name is historical):
RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
while [ $RDY1 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY1=`ss-get --timeout 3600 scheduler.1:nfs_ready`
done

mount -t cifs //`ss-get --timeout 3600 scheduler.1:hostname`/share /mnt/nfs -o username=guest,password=''

# Wait for scheduler:
RDY2=`ss-get --timeout 3600 scheduler.1:sched_ready`
while [ $RDY2 != "1" ]; do
    echo "Waiting for the scheduler..."
    sleep 1
    RDY2=`ss-get --timeout 3600 scheduler.1:sched_ready`
done

# Build JobRunner (must happen after the sched starts, as it downloads Queue)
cd ~/scan/queue_jobrunner
scalac -cp /mnt/nfs/Queue-3.1-smowton.jar:/root/scan/json-org.jar *.scala

# Enable passwordless SSH access (for example?)
mkdir ~/.ssh

# auth_keys might not end with a newline at the moment
echo >> ~/.ssh/authorized_keys
echo `ss-get --timeout 3600 scheduler.1:authorized_keys | base64 -d` >> ~/.ssh/authorized_keys

# Register to run user-submitted jobs
SCHED_ADDRESS=`ss-get --timeout 3600 scheduler.1:sched_address`
echo $SCHED_ADDRESS > ~/scan/scheduler_address
~/scan/register_worker.py $SCHED_ADDRESS gatk_queue_runner > ~/scan_worker_id
~/scan/register_worker.py $SCHED_ADDRESS gatk_queue_runner > ~/scan_worker_id2
~/scan/register_worker.py $SCHED_ADDRESS gatk_queue_runner > ~/scan_worker_id3
~/scan/register_worker.py $SCHED_ADDRESS gatk_queue_runner > ~/scan_worker_id4
