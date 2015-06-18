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

    return "%.1g%sb" % (nbytes, suffix[power])

img_file = "/tmp/disk.img"

# Urgh, racy.
loopdev = subprocess.check_output(["/sbin/losetup", "-f"]).strip()
statvfs = os.statvfs('/')

alloc_blocks = int(statvfs.f_bavail * 0.8)
print "Allocating", alloc_blocks, "*", statvfs.f_bsize, "blocks (%s)" % pp_bytes(statvfs.f_bsize * alloc_blocks)
subprocess.check_call(["/bin/dd", "if=/dev/zero", "of=%s" % img_file, "bs=%d" % statvfs.f_bsize, "count=%d" % alloc_blocks])
subprocess.check_call(["/sbin/losetup", loopdev, img_file])

btrfs_common.add_device(loopdev)


