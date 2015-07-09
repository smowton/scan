#!/usr/bin/env python

import sys
import subprocess
import btrfs_common
import os

def pp_bytes(nbytes):

    suffix = ["", "k", "M", "G", "T"]
    power = 0
    nbytes = float(nbytes)
    while nbytes > 1024:
        nbytes = nbytes / 1024
        power += 1

    return "%.1f%sB" % (nbytes, suffix[power])

img_file = "/tmp/disk.img"

# Urgh, racy.
loopdev = subprocess.check_output(["/sbin/losetup", "-f"]).strip()
statvfs = os.statvfs('/')

alloc_blocks = int(statvfs.f_bavail * 0.8)
alloc_bytes = statvfs.f_bsize * alloc_blocks
print "Allocating %s of root partition to btrfs" % pp_bytes(alloc_bytes)
subprocess.check_call(["/usr/bin/fallocate", "-l", str(alloc_bytes), img_file])
subprocess.check_call(["/sbin/losetup", loopdev, img_file])

btrfs_common.add_device(loopdev)

# Write the initial device DB

devs = btrfs_common.get_devs()
db = {"non-dynamic-%d" % i: d for (i, d) in enumerate(devs)}
btrfs_common.write_id_db(db)

