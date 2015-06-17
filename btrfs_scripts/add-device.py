#!/usr/bin/env python

import os
import subprocess
import sys

import btrfs_common
mountpoint = btrfs_common.mountpoint

if len(sys.argv) != 2:
    print >>sys.stderr, "Usage: add-device.py device-uuid"
    sys.exit(1)

devname = "/dev/disk/by-uuid/" + sys.argv[1]

if not os.path.exists(mountpoint):

    try:
        os.mkdir(mountpoint)
    except OSError as e:
        if e.errno != 17:
            raise e

    subprocess.check_call(["/sbin/mkfs.btrfs", "-d", "single", devname])
    subprocess.check_call(["/sbin/mount", devname, mountpoint])

else:
    
    subprocess.check_call(["/sbin/btrfs", "device", "add", devname, mountpoint])

