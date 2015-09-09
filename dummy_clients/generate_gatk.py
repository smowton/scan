
import subprocess
import generate_helpers
import tempfile
import os.path
import shutil
import random
import math

def upload_gatk_refdata(server, basedir):

    upload_files = ["ref.fa", "ref.dict", "ref.fa.fai", "dbsnp.vcf", "dbsnp.vcf.idx"]
    for f in upload_files:
        localpath = os.path.join(basedir, f)
        remotepath = os.path.join("gatk_refs", f)
        generate_helpers.push_file(server, localpath, remotepath, may_exist = True)

def start_gatk(server, basefile, sizefraction, runid):

    td = tempfile.mkdtemp()
    dfsname = "gatk_%d/in.bam" % runid

    if sizefraction == 1.0:
        generate_helpers.push_file(server, basefile, dfsname)
    else:
        reducedfile = os.path.join(td, "in.bam")
        print "Generate reduced bam..."
        with open(reducedfile, "w") as f:
            subprocess.check_call(["samtools", "view", basefile, "-s", str(sizefraction + runid), "-b"], stdout = f)
        print "Index input bam..."
        subprocess.check_call(["samtools", "index", reducedfile])
        generate_helpers.push_file(server, reducedfile, dfsname)
        generate_helpers.push_file(server, reducedfile + ".bai", dfsname + ".bai")

    scanfs_workdir = os.path.join("/mnt/scanfs", os.path.dirname(dfsname))
    scanfs_input = os.path.join(scanfs_workdir, "in.bam")

    queue_args = ["--input", scanfs_input,
                  "--workdir", scanfs_workdir,
                  "--refdir", "/mnt/scanfs/gatk_refs",
                  "--estsize", "%d" % int(math.floor(sizefraction * 100))]

    ret = generate_helpers.start_queue_task(server, "/home/user/scan/queue_scripts/gatk_pipeline.scala", queue_args)

    shutil.rmtree(td)

    return ret

def cleanup_gatk(server, runid):

    to_del = ["in.bam.bai", "in.bam", "realign_targets.intervals", "realigned.bam", "recal.csv", "recal.bam", "unfiltered_calls.vcf", "filtered_calls.vcf", "final_calls.vcf"]

    for d in to_del:
        generate_helpers.del_file(server, os.path.join("gatk_%d" % runid, d))
    
