#!/bin/bash

case $SLIPSTREAM_SCALING_ACTION in
  vm_remove)
      ~/scan/scripts/del_this_worker.sh ;;
  disk_detach)
      python ~/scan/btrfs_scripts/remove-device.py `ss-get disk.detach.device` ;;
esac