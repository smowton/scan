#!/bin/bash

if [ "$#" -ne 5 ]; then
    echo "Usage: start_gatk_pipeline.sh input.bam scatter_count refdir workdir queuejar"
    exit 1
fi

CWD=`pwd`

INBAM=$1
SCATTER=$2
REFDIR=$3
WORKDIR_ROOT=$4
QUEUEJAR=$5

WORKDIR=`TMPDIR=$WORKDIR_ROOT mktemp -d -t scan_gatk_workdir.XXXXXXXX` || exit 1
JAVATMPDIR=$WORKDIR/java_tmp
QUEUETMPDIR=$WORKDIR/qsg
mkdir $JAVATMPDIR || exit 1
mkdir $QUEUETMPDIR || exit 1

cd $WORKDIR && java -Djava.io.tmpdir=$JAVATMPDIR -cp $QUEUEJAR:$CWD/queue_jobrunner:$CWD/json-org.jar org.broadinstitute.sting.queue.QCommandLine -S $CWD/queue_scripts/gatk_pipeline.scala -jobRunner Scan -run -startFromScratch -jobSGDir $QUEUETMPDIR --workdir $WORKDIR --input $INBAM --scattercount $SCATTER --singlequeue --refdir $REFDIR
