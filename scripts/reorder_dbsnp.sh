#!/bin/bash

cat $1 | head -n 167 > /tmp/header
cat $1 | tail -n +168 | head -n 475 > /tmp/mt
tail -n +643 $1 | cat /tmp/header - /tmp/mt > $1.new
mv $1.new $1
