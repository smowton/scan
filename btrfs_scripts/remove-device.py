
#!/usr/bin/env python

import os
import subprocess
import sys

import btrfs_common
mountpoint = btrfs_common.mountpoint

if len(sys.argv) != 2:
    print >>sys.stderr, "Usage: remove-device.py device-uuid"
    sys.exit(1)

devname = "/dev/disk/by-uuid/" + sys.argv[1]


subprocess.check_call(["/sbin/btrfs", "device", "add", devname, mountpoint])
