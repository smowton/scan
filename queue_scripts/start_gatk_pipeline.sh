#!/bin/bash

WORKDIR=`TMPDIR=/mnt/nfs/gatk/intermediate/ mktemp -d -t scan_gatk_workdir.XXXXXXXX` || exit 1
JAVATMPDIR=$WORKDIR/java_tmp
mkdir $JAVATMPDIR || exit 1

java -Djava.io.tmpdir=$JAVATMPDIR -cp /mnt/nfs/gatk/gatk-git/target/Queue.jar:/home/user/csmowton/scan/queue_jobrunner:/home/user/csmowton/json-org.jar org.broadinstitute.sting.queue.QCommandLine -S gatk_pipeline.scala -jobRunner Scan -run -startFromScratch --workdir $WORKDIR --input $1