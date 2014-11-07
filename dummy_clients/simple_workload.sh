#!/bin/bash

CWD=`pwd`
while true; do
	$CWD/dummy_clients/start_gatk_pipeline.sh "$@"
done
