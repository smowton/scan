#!/usr/bin/env python

import sys
import btrfs_common
import time

if len(sys.argv) != 2:
    print >>sys.stderr, "Usage: add-device.py new-dev-uuid"
    sys.exit(1)

db = btrfs_common.read_id_db()

known_disks = db.values()

for repeats in range(10):

    new_disks = [d for d in btrfs_common.get_devs() if d not in known_disks]
    if len(new_disks) >= 2:
        raise Exception("Many new devices: I don't know which of %s to associate with given UUID %s" % (new_disks, sys.argv[1]))
    elif len(new_disks) == 1:

        btrfs_common.add_device(new_disks[0])
        db[sys.argv[1]] = new_disks[0]
        btrfs_common.write_id_db(db)
        sys.exit(0)

    else:
        time.sleep(5)

print >>sys.stderr, "Timed out waiting for a new disk (not one of %s) to appear" % known_disks
sys.exit(1)
