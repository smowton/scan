
import httplib

def start_queue_task(server, qscript, qargs):

    cmd = "java -cp /mnt/nfs/Queue-3.1-smowton.jar:/home/user/scan/json-org.jar:. org.broadinstitute.sting.queue.QCommandLine -S %s -jobRunner Scan -run -tempDir /mnt/nfs/queue/ -logDir /mnt/nfs/queue/ -jobSGDir /mnt/nfs/queue/ %s" % (qscript, " ".join(qargs))

    post_args = {"cmd": cmd,
                 "classname": "queue_runner",
                 "maxcores": "1",
                 "mempercore": "1",
                 "estsize": "1",
                 "filesin": "",
                 "filesout": ""}

    post_args = urllib.urlencode(post_args)

    headers = {"Content-type": "application/x-www-form-urlencoded"}

    conn = httplib.HTTPConnection(server, 8080)
    conn.request("POST", "/addworkitem", post_args, headers)
    response = conn.getresponse()
    if response.status != 200:
        raise Exception("Addworkitem %s / %s failed with code %s / %s" % (qscript, qargs, response.status, response.reason))

def push_file(server, localname, dfsname):

    with open(localname, "r") as f:
        conn = httplib.HTTPConnection(server, 8080)
        conn.request("PUT", "/dfsput?path=" + dfsname, f)
        response = conn.getresponse()
        if response.status != 200:
            raise Exception("Put %s to %s failed with code %s / %s" % (localname, dfsname, response.status, response.reason))

def del_file(server, dfsname):

    
