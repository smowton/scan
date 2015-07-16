
import os
import os.path
import subprocess
import json
import glob

mountpoint = "/mnt/scanfs"

def add_device(devname):

    if not os.path.exists(mountpoint):

        try:
            os.mkdir(mountpoint)
        except OSError as e:
            if e.errno != 17:
                raise e

        subprocess.check_call(["/sbin/mkfs.btrfs", "-d", "single", devname])
        subprocess.check_call(["/bin/mount", devname, mountpoint])

    else:

        subprocess.check_call(["/sbin/btrfs", "device", "add", devname, mountpoint])

db_location = "/tmp/btrfs_device_ids"

def get_devs():
    return glob.glob("/dev/vd?")

def read_id_db():

    with open(db_location, "r") as f:
        return json.load(f)

def write_id_db(db):

    with open(db_location, "w") as f:
        json.dump(db, f)
        
