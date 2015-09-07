
import subprocess
import generate_helpers
import tempfile
import os.path
import shutil
import random

def start_gatk(server, basefile, sizefraction, runid):

    td = tempfile.mkdtemp()
    dfsname = "gatk_%d/in.bam" % runid

    if sizefraction == 1.0:
        generate_helpers.push_file(server, basefile, dfsname)
    else:
        reducedfile = os.path.join(td, "in.bam")
        sizefraction = str(sizefraction)
        sizefraction = sizefraction[sizefraction.find(".") + 1:]
        with open(reducedfile, "w") as f:
            subprocess.check_call(["samtools", "view", basefile, "-s", "%d.%s" % (random.randint(0, 1000000), sizefraction), "-b"], stdout = f)
        generate_helpers.push_file(server, reducedfile, dfsname)

    scanfs_workdir = os.path.join("/mnt/scanfs", os.path.dirname(dfsname))
    scanfs_input = os.path.join(scanfs_workdir, "in.bam")

    queue_args = ["--input=%s" % scanfs_input,
                  "--workdir=%s" % scanfs_workdir,
                  "--refdir=/mnt/scanfs/gatk_refs",
                  "--estsize=%f" % sizefraction]

    generate_helpers.start_queue_task(server, "/home/user/scan/queue_scripts/gatk_pipeline.scala", queue_args)

    shutil.rmtree(td)
