#!/bin/bash

# Arg 1 is the job directory

runner_for_job=$(python ./get_cluster_hostname.py ~/noflex_cluster.json $1)

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -R 8080:localhost:8080 $runner_for_job java -Djava.io.tmpdir=$1 -cp /mnt/nfs/gatk/Queue-3.1-smowton.jar:/mnt/nfs/gatk/json-org.jar:/mnt/nfs/gatk/queue_jobrunner org.broadinstitute.sting.queue.QCommandLine -S /mnt/nfs/gatk/gatk_pipeline_nowrite.scala -run -jobSGDir $1 --scattercount 1 --input /mnt/nfs/inputs/226_normal.gatk.bam --workdir /mnt/nfs/inputs/inter_normal/ --fakedir $1 --singlequeue -jobRunner Scan

