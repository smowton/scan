
import tempfile
import os.path
import generate_helpers
import shutil

def start_gromacs(server, indir, runid, timefraction):

    td = tempfile.mkdtemp()
    
    dfsworkdir = "/gromacs_%d" % runid
    steps_defaults = {"EM": 50000, "NVT": 50000, "NPT": 50000, "main": 50000}

    for k, v in steps_defaults.iteritems():

        localname = os.path.join(td, "%s.mdp" % k)
        templatename = os.path.join(indir, "%s.mdp" % k)
        with open(localname, "w") as fout, open(templatename, "r") as fin:
            fout.write(fin.read().replace("NSTEPS", str(v * timefraction)))
        generate_helpers.push_file(server, localname, os.path.join(dfsworkdir, "%s.mdp" % k))
            
    upload_files = ["input.top", "input.gro", "posre.itp"]
    for f in upload_files:
        generate_helpers.push_file(server, os.path.join(indir, f), os.path.join(dfsworkdir, f))

    queue_args = ["--workdir", os.path.join("/mnt/scanfs", dfsworkdir),
                  "--estsize", str(timefraction)]

    generate_helpers.start_queue_task(server, "/home/user/scan/queue_scripts/gromacs_pipeline.scala", queue_args)
    
    shutil.rmtree(td)
        
