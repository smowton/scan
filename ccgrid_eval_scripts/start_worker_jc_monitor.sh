#!/bin/bash

cd /mnt/nfs/jc/cloud-ms/JCatascopia-Agent/home_worker
java -jar ../target/JCatascopia-Agent-0.0.1-SNAPSHOT.jar > ~/jc-metrics.log 2>&1 &
