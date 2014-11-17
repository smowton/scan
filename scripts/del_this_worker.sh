#!/bin/bash

# Releases any worker in the given class, blocks until it is ready to stop (perhaps a long time) and prints its details in the format classname:hostname:id

~/scan/blocking_delete.py $1 `cat ~/scan_worker_id`



