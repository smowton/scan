
import os
import os.path
import subprocess

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
