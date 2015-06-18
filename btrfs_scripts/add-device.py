#!/usr/bin/env python

import sys
import btrfs_common

if len(sys.argv) != 2:
    print >>sys.stderr, "Usage: add-device.py device-uuid"
    sys.exit(1)

devname = "/dev/disk/by-uuid/" + sys.argv[1]

btrfs_common.add_device(devname)
