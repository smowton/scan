#!/bin/bash

if [ $# > 1 ]; then
   CUT_FACTOR=$1
else
   CUT_FACTOR=0.5
fi

curl http://localhost:8080/notifypressure?cut_factor=$CUT_FACTOR