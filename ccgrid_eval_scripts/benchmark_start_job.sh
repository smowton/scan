#!/bin/bash

# Arg 1 is the job directory
# Arg 2 is a net-accessible temp directory suited to small files, quick create/delete, etc.

base=$(dirname $0)

runner_for_job=$(python $base/get_cluster_hostname.py /tmp/gm.json $1)

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $runner_for_job SCAN_HOST=192.168.0.1 java -cp /mnt/nfs/gatk/Queue-3.1-smowton.jar:/mnt/nfs/gatk/json-org.jar:/mnt/nfs/gatk/queue_jobrunner org.broadinstitute.sting.queue.QCommandLine -S /mnt/nfs/gatk/gatk_pipeline.scala -run -jobSGDir $1 -runDir $2 --scattercount 1 --input /mnt/nfs/inputs/in.bam --workdir $1 --singlequeue -jobRunner Scan

