#!/usr/bin/env python

import os
import subprocess
import sys

import btrfs_common
mountpoint = btrfs_common.mountpoint

if len(sys.argv) != 2:
    print >>sys.stderr, "Usage: remove-device.py device-uuid"
    sys.exit(1)

id_db = btrfs_common.read_id_db()
devname = id_db[argv[1]]
del devname[argv[1]]

out = subprocess.check_output(["/sbin/btrfs", "filesystem", "show"])

lines = out.split("\n")

ndevs = None

for l in lines:
	if l.find("Total devices") != -1:
		bits = l.split()
		for i in range(len(bits)):
			if bits[i] == "Total":
				ndevs = int(bits[2])
	elif l.find("path %s" % devname) != -1:
		# This is the FS of interest
		break

if ndevs is None:
	raise Exception("btrfs filesystem show didn't describe an FS containing %s. Output:\n\n%s" % (devname, out))

subprocess.check_call(["/sbin/btrfs", "device", "delete", devname, mountpoint])

btrfs_common.write_id_db(id_db)


