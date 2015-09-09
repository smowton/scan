#!/bin/bash

JAVABINPATH=`which java`
ABSJAVAPATH=`readlink -f $JAVABINPATH`
export JAVA_HOME=`dirname $ABSJAVAPATH`/../..
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$LD_LIBRARY_PATH

mkdir -p $3
mkdir -p $4

ESCAPED_INPUT=`echo $2 | sed s/\\\\//\\\\\\\\\\\\//g`
cp /home/user/scan/cp_scripts/headless.cppipe $2
cp /home/user/scan/cp_scripts/imageset.csv $2
sed -i s/INPUTPATH/$ESCAPED_INPUT/g $2/imageset.csv
sed -i s/INPUTPATH/$ESCAPED_INPUT/g $2/headless.cppipe

python $1/CellProfiler/CellProfiler.py -p $2/headless.cppipe -c -r -b --do-not-fetch -o $3 -t $4
