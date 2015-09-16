#!/bin/bash

case $SLIPSTREAM_SCALING_ACTION in
  disk_attach)
      python ~/scan/btrfs_scripts/add-device.py `ss-get disk.attached.device` ;;
esac

