#!/bin/bash

mkdir -p $3
mkdir -p $4

ESCAPED_INPUT=`echo $2 | sed s/\\\\//\\\\\\\\\\\\//g`
sed -i s/INPUTPATH/$ESCAPED_INPUT/g $2/imageset.csv
sed -i s/INPUTPATH/$ESCAPED_INPUT/g $2/headless.cppipe

python $1/CellProfiler/CellProfiler.py -p $2/headless.cppipe -c -r -b --do-not-fetch -o $3 -t $4
