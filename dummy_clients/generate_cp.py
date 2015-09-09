
import tempfile
import os.path
import generate_helpers
import shutil
import subprocess

def start_cp(server, infile, runid, pixelsize):

    dfspath = "/cp_%d" % runid
    td = tempfile.mkdtemp()

    for i in range(3):
        localname = os.path.join(td, "IN%d.tif" % i)	
        subprocess.check_call(["convert", infile, "-resize", "%dx%d" % pixelsize, localname])
        generate_helpers.push_file(server, localname, os.path.join(dfspath, "IN%d.tif" % i))

    queue_args = ["--indir=%s" % os.path.join("/mnt/scanfs", dfspath),
                  "--outdir=%s" % os.path.join("/mnt/scanfs", dfspath, "out"),
                  "--tmpdir=%s" % os.path.join("/mnt/scanfs", dfspath, "tmp"),
                  "--estsize=%d" % (pixelsize ** 2)]

    generate_helpers.start_queue_task(server, "/home/user/scan/queue_scripts/cp_pipeline.scala", queue_args)
