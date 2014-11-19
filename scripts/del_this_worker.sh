#!/bin/bash

# Releases this worker. Blocks until it is ready to stop (perhaps a long time) and prints its details in the format classname:hostname:id

~/scan/blocking_delete.py `cat ~/scan_worker_class` `cat ~/scan_sched_address` `cat ~/scan_worker_id`



