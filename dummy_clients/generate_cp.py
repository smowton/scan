
import tempfile
import os.path
import generate_helpers
import shutil
import subprocess

def start_cp(server, indir, runid, pixelsize):

    dfspath = "cp_%d" % runid
    td = tempfile.mkdtemp()

    for i in range(3):
        localname = os.path.join(td, "IN%d.tif" % i)	
        subprocess.check_call(["convert", os.path.join(indir, "IN%d.tif" % i), "-resize", "%dx%d" % (pixelsize, pixelsize), localname])
        generate_helpers.push_file(server, localname, os.path.join(dfspath, "IN%d.tif" % i))

    queue_args = ["--indir", os.path.join("/mnt/scanfs", dfspath),
                  "--outdir", os.path.join("/mnt/scanfs", dfspath, "out"),
                  "--tmpdir", os.path.join("/mnt/scanfs", dfspath, "tmp"),
                  "--estsize", "%d" % (pixelsize ** 2)]

    ret = generate_helpers.start_queue_task(server, "/root/scan/queue_scripts/cp_pipeline.scala", queue_args)

    shutil.rmtree(td)

    return ret

def get_results(server, runid, localname):

    generate_helpers.get_file(server, "cp_%d/out/Outline_thumbnail.png" % runid, localname)

def cleanup_cp(server, runid):

    rundir = "/cp_%d" % runid
    todel = ["IN%d.tif" % i for i in range(3)] + ["out/IN0_Outline.png", "out/Outline_thumbnail.png"]
    for d in todel:
        generate_helpers.del_file(server, os.path.join(rundir, d))

