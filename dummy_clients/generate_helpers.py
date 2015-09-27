
import httplib
import urllib
import json
import time
import shutil
import sys

def start_queue_task(server, qscript, qargs):

    cmd = "java -cp /mnt/nfs/Queue-3.1-smowton.jar:/root/scan/json-org.jar:/root/scan/queue_jobrunner org.broadinstitute.sting.queue.QCommandLine -S %s -jobRunner Scan -run -tempDir /mnt/nfs/queue/ -logDir /mnt/nfs/queue/ -jobSGDir /mnt/nfs/queue/ %s" % (qscript, " ".join(qargs))

    post_args = {"cmd": cmd,
                 "classname": "queue_runner",
                 "maxcores": "0",
                 "mempercore": "1",
                 "estsize": "1",
                 "filesin": "",
                 "filesout": ""}

    post_args = urllib.urlencode(post_args)

    headers = {"Content-type": "application/x-www-form-urlencoded"}

    print "Start", qscript, qargs

    conn = httplib.HTTPConnection(server, 8080)
    conn.request("POST", "/addworkitem", post_args, headers)
    response = conn.getresponse()
    if response.status != 200:
        raise Exception("Addworkitem %s / %s failed with code %s / %s" % (qscript, qargs, response.status, response.reason))

    return int(json.load(response)["pid"])

def wait_for_task(server, pid):

    while True:

        print "Poll task", pid
	try:
            conn = httplib.HTTPConnection(server, 8080)
            conn.request("GET", "/lscompletedprocs")
            response = conn.getresponse()
            if response.status != 200:
                raise Exception("Poll %d failed with code %s / %s" % (pid, response.status, response.reason))    
        except Exception as e:
            print >>sys.stderr, "Failed to poll %s" % e
            continue

        tasks = json.load(response)
        tasks = dict([(int(k), v["return"]) for (k, v) in tasks.iteritems()])

        if pid in tasks:
            if tasks[pid] == 0:
                print "Task completed successfully"
                return
            else:
                raise Exception("Task %d exited with return code %d" % (pid, tasks[pid]))

        time.sleep(5)

dfscontents = None

def push_file(server, localname, dfsname, may_exist = False):

    print localname, "->", dfsname

    global dfscontents
    if dfscontents is None:
        conn = httplib.HTTPConnection(server, 8080)
        conn.request("GET", "/dfsfind")
        response = conn.getresponse()
        if response.status != 200:
            raise Exception("dfsfind with code %s / %s" % (response.status, response.reason))
        dfscontents = json.loads(response.read())

    if dfsname in dfscontents and may_exist:
        print "(Already exists)"
        return

    with open(localname, "r") as f:
        conn = httplib.HTTPConnection(server, 8080)
        conn.request("PUT", "/dfsput?path=" + dfsname, f)
        response = conn.getresponse()
        if response.status != 200:
            raise Exception("Put %s to %s failed with code %s / %s" % (localname, dfsname, response.status, response.reason))
        dfscontents[dfsname] = 1

def get_file(server, dfsname, localname):

    conn = httplib.HTTPConnection(server, 8080)	
    conn.request("GET", "/dfsget?path=" + dfsname)
    response = conn.getresponse()
    if response.status != 200:
        raise Exception("Get %s failed with code %s / %s" % (dfsname, response.status, response.reason))
    with open(localname, "w") as fout:
        shutil.copyfileobj(response, fout)
    
def del_file(server, dfsname):

    headers = {"Content-type": "application/x-www-form-urlencoded"}
    post_args = urllib.urlencode({"path": dfsname})
    conn = httplib.HTTPConnection(server, 8080)		
    conn.request("POST", "/dfsrm", post_args, headers)
    response = conn.getresponse()
    if response.status != 200:
        raise Exception("DFS rm %s failed with code %s / %s" % (dfsname, response.status, response.reason))
    
