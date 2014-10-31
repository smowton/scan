#!/bin/bash

# Arg 1 is the job directory

base=$(dirname $0)

runner_for_job=$(python $base/get_cluster_hostname.py ~/ccgrid_nodes.json $1)

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -R 8080:localhost:8080 $runner_for_job java -Djava.io.tmpdir=$1 -cp /lustre/scratch/csmowton/gatk-benchmark/gatk/Queue-3.1-smowton.jar:/lustre/scratch/csmowton/gatk-benchmark/gatk/json-org.jar:/home/csmowton/tmp/scan/queue_jobrunner org.broadinstitute.sting.queue.QCommandLine -S /home/csmowton/tmp/scan/queue_scripts/gatk_pipeline.scala -run -jobSGDir $1 --scattercount 1 --input /lustre/scratch/csmowton/gatk-benchmark/inputs/226_normal.gatk.bam --workdir $1 --singlequeue -jobRunner Scan

