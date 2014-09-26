#!/bin/bash

sudo apt-get install python python-pip python-dev git
sudo pip install cql cherrypy

# Get the scheduler code etc:
cd ~
git clone https://github.com/smowton/scan.git

# Note the DB's master node address:
ss-get cassandra-address > ~/scan/cassandra_address

# Create SSH keys and publish the public key for workers to use:
mkdir -p ~/.ssh
ssh-keygen -N "" -f ~/.ssh/id_rsa
ss-put authorized_keys < ~/.ssh/id_rsa.pub

# Start the scheduler
~/scan/tinysched.py &
~/scan/await_server.py

# Note that we're ready
ss-put sched_address `hostname -f`
ss-put sched_ready 1
