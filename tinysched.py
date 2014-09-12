#!/usr/bin/python

import cherrypy
import threading
import sys
import json
import copy
import subprocess
import httplib
import urllib
import Queue
import datetime
import traceback

import scan_templates

runner = ["/usr/bin/ssh"]

class Worker:

	def __init__(self, address, hwsv, wid):
		self.address = address
                self.hwspec_version = hwsv
                self.wid = wid

        def to_dict(self):
                return self.__dict__

class Task:

	def __init__(self, cmd, pid, sched):
		self.cmd = cmd
                self.pid = pid
		self.proc = None
		self.worker = None
                self.start_time = None
                self.sched = sched

        def to_dict(self):
                d = copy.deepcopy(self.__dict__)
                if self.proc is not None:
                        d["proc"] = self.proc.pid
                if self.worker is not None:
                        d["worker"] = self.worker.address
                return d

        def piddir(self):
                return "/tmp/scanpids/%d" % self.pid

        def pidfile(self):
                return "%s/pid" % self.piddir()

        def getresusage(self):
                cmd = copy.deepcopy(runner)
                cmd.append("%s@%s" % (self.sched.classspec["user"], self.worker.address))
                cmd.append(self.sched.classspec["respath"])
                cmd.append(self.pidfile())
                jsstats = json.loads(subprocess.check_output(cmd))
                if "error" in jsstats:
                        raise Exception(jsstats["error"])
                else:
                        return jsstats
                
class DictEncoder(json.JSONEncoder):

        def default(self, o):
                return o.to_dict()

class SubmitUI:

        def __init__(self, classes):
                self.classes = classes

        def mkcombo(self, name, entries):
                return ('<select name="%s"><option value="null"></option>' % name
                        + "".join(['<option value="%s">%s</option>' % (d["name"], d["desc"]) for d in entries])
                        + '</select>')

        def mktemplatecombo(self):
                return self.mkcombo("template", [{"name": k, "desc": v["desc"]} for (k, v) in scan_templates.templates().iteritems()])
        
        def mkclasscombo(self):
                return self.mkcombo("classname", [{"name": c["name"], "desc": c["name"]} for c in self.classes])

        @cherrypy.expose
        def index(self):
                return """

<html><body><h3>SCAN Job Submission</h3>
<form method="post" action="/addworkitem_ui">
<p><table><tr><td>
<p><b>Select a template, and enter input values</b></p>
<p>Template: %s</p>
<p>Input values (one per line):</p>
<p><textarea name="templateinputs" rows="8" cols="40"></textarea></p>
</td><td>
<p><b>...or, enter your script and pick a task class</b></p>
<p>Script:</p>
<p><textarea name="script" rows="8" cols="40"></textarea></p>
<p>Class: %s</p>
</td></tr>
<tr><td><input type="submit"/></td></tr></table></p>
</form></body></html>""" % (self.mktemplatecombo(), self.mkclasscombo())

class MulticlassScheduler:

        def __init__(self, httpqueue):

                # TODO: stop hardcoding classes
                classes = [{"name": "linux",
                            "user": "user",
                            "respath": "/home/user/csmowton/scan/getres.py"},
                           {"name": "windows",
                            "user": "Administrator",
                            "respath": "/home/Administrator/getres.py"}]
                self.queues = dict()
                for c in classes:
                        self.queues[c["name"]] = TinyScheduler(c, httpqueue)

                self.ui = SubmitUI(classes)

        @cherrypy.expose
        def addworkitem_ui(self, template, templateinputs, classname, script):

                try:

                        if template != "null":

                                if classname != "null":
                                        raise Exception("Must specify either a template or a class, not both")
                                try:
                                        temp = scan_templates.templates()[template]
                                except KeyError:
                                        raise Exception("No such template %s" % template)
                                classname = temp["classname"]
                                templateinputs = [x.strip() for x in templateinputs.split("\n") if len(x.strip()) > 0]
                                if len(templateinputs) == 0:
                                        raise Exception("Must specify at least one template parameter")
                                scripts = [temp["script"] % t for t in templateinputs]

                        elif classname != "null":

                                scripts = [script]

                        else:

                                raise Exception("Must specify either a template or a class")

                        pids = []

                        for s in scripts:
                                ret = self.default(callname="addworkitem", classname=classname, cmd=s, fsreservation=0, dbreservation=0, set_ct=False)
                                pids.append(json.loads(ret)["pid"])
                              
                        return "<html><body><p>Created PIDs %s</p></body></html>" % ", ".join([str(p) for p in pids])

                except Exception as e:

                        return "<html><body><p>Error: %s</p><p>%s</p></body></html>" % (e, traceback.format_exc(e).replace("\n", "<br/>"))

        @cherrypy.expose
        def default(self, callname, classname, **kwargs):
                
                if "set_ct" not in kwargs:
                        cherrypy.response.headers['Content-Type'] = "application/json"
                else:
                        del kwargs["set_ct"]

                try:
                        q = self.queues[classname]
                except KeyError:
                        return json.dumps({"error": "No such class %s" % classname})

                try:
                        call = getattr(q, callname)
                except AttributeError:
                        return json.dumps({"error": "No such method %s" % callname})

                return call(**kwargs)

class TinyScheduler:
 
        def __init__(self, classspec, httpqueue):
                
                self.procs = dict()
                self.pending = []
                self.workers = dict()
                self.freeworkers = []
                self.pendingreplacements = []
                self.pendingremoves = 0
                self.current_hwspec_version = 0
                self.current_hwspec = dict()
                self.nextpid = 0
                self.nextwid = 0
                self.classspec = classspec
                self.callback_address = None
                self.callback_host = None
                self.httpqueue = httpqueue
                self.lock = threading.Lock()

        def trystartproc(self):

                if len(self.pending) == 0:
                        return False
                        
                if len(self.freeworkers) == 0:
                        return False

                runworker = self.freeworkers[-1]
                self.freeworkers.pop()

                cmdline = copy.deepcopy(runner)
                cmdline.append("%s@%s" % (self.classspec["user"], runworker.address))

                np = self.pending[0]

                cmd = np.cmd
                for key, val in self.current_hwspec.iteritems():
                        cmd = cmd.replace("%%%%%s%%%%" % key, val)

                # Prepend bookkeeping:
                cmd = "mkdir -p %s; echo $$ > %s; %s" % (np.piddir(), np.pidfile(), cmd)

                cmdline.append(cmd)
                
                print "Start process", np.pid, cmdline

                np.proc = subprocess.Popen(cmdline)
                np.worker = runworker
                np.start_time = datetime.datetime.now()
                self.pending.pop(0)

                return True

	def trystartprocs(self):

                while self.trystartproc():
                        pass

        def release_worker(self, freed_worker, in_workers_map = True):

                # Worker has already been removed from the free list.
                if in_workers_map:
                        del self.workers[freed_worker.wid]
                
                if self.callback_host is None:
                        return

                params = {"class": self.classspec["name"], "wid": freed_worker.wid}
                self.httpqueue.queue.put((self.callback_host, self.callback_address, params))

                print "Release worker callback for", freed_worker.wid, freed_worker.address

	def pollworkitem(self, pid):

		with self.lock:

			pid = int(pid)
			rp = self.procs[pid]
			if rp.proc is not None:
				ret = rp.proc.poll()
			else:
				ret = None
			if ret is not None:

                                print "Task", pid, "finished"
                                
				del self.procs[pid]
                                freed_worker = rp.worker

                                if self.pendingremoves > 0:
                                        print "Releasing worker", freed_worker.wid, freed_worker.address
                                        self.release_worker(freed_worker)
                                        self.pendingremoves -= 1
                                        freed_worker = None
                                elif freed_worker.hwspec_version != self.current_hwspec_version:
                                        if len(self.pendingreplacements) == 0:
                                                raise Exception("No pending replacements, but worker %s version %d does not match current version %d?" % 
                                                                (freed_worker.address, freed_worker.hwspec_version, self.current_hwspec_version))
                                        print "Replacing worker", freed_worker.wid, freed_worker.address, "with", self.pendingreplacements[-1].wid, self.pendingreplacements[-1].address
                                        self.release_worker(freed_worker)
                                        freed_worker = self.pendingreplacements[-1]
                                        self.workers[freed_worker.wid] = freed_worker
                                        self.pendingreplacements.pop()
                                        
                                if freed_worker is not None:
                                        self.freeworkers.append(freed_worker)
					self.trystartprocs()

			return json.dumps({"pid": pid, "retcode": ret})

        def addworkitem(self, cmd, fsreservation, dbreservation):

		with self.lock:

			newpid = self.nextpid
			newproc = Task(cmd, newpid, self)
			self.nextpid += 1
			self.procs[newpid] = newproc
			self.pending.append(newproc)
                        print "Queue work item", newpid, cmd
			self.trystartprocs()
			return json.dumps({"pid": newpid})

        def newworker(self, address):
                
                wid = self.nextwid
                self.nextwid += 1
                print "Add worker", wid, address
                return Worker(address, self.current_hwspec_version, wid)

	def addworker(self, address):

		with self.lock:

                        newworker = self.newworker(address)
                        self.workers[newworker.wid] = newworker
                        self.freeworkers.append(newworker)
			self.trystartprocs()
			return json.dumps({"wid": newworker.wid})

	def delworker(self):

		with self.lock:

                        if len(self.workers) == 0:
                                raise Exception("No workers in this class pool")
                        if len(self.freeworkers) == 0:
                                self.pendingremoves += 1
                        else:
                                todel = self.freeworkers[-1]
                                self.freeworkers.pop()
                                self.release_worker(todel)
			return json.dumps({"status": "ok"})

	def modhwspec(self, addresses, newspec):

		with self.lock:

                        addresses = [x.strip() for x in addresses.split(",") if len(x.strip()) > 0]
                        if len(addresses) != len(self.workers):
                                raise Exception("Must supply the same number of replacement workers as are currently in the pool (supplied %d, have %d)" % (len(addresses), len(self.workers)))
                                
                        specs = [x.strip() for x in newspec.split(",") if len(x.strip()) > 0]
                        newdict = dict()
                        for spec in specs:
                                bits = spec.split(":")
                                if len(bits) != 2:
                                        raise Exception("newspec parameter syntax: k1:v1,k2:v2...")
                                newdict[bits[0]] = bits[1]
                        self.current_hwspec = newdict

                        self.current_hwspec_version += 1
                        new_workers = [self.newworker(a) for a in addresses]
                        new_worker_ids = [w.wid for w in new_workers]

                        # Step 1: immediately release any pendingreplacements, which never
                        # made it into the live worker pool.
                        for w in self.pendingreplacements:
                                self.release_worker(w, False)

                        # Step 2: immediately replace any free workers.
                        freeworkers = self.freeworkers
                        self.freeworkers = []
                        for w in freeworkers:
                                self.release_worker(w)
                                replace_with = new_workers[-1]
                                new_workers.pop()
                                self.workers[replace_with.wid] = replace_with
                                self.freeworkers.append(replace_with)

                        print "Replaced", len(self.workers) - len(new_workers), "workers immediately;", len(new_workers), "pending"

                        # Step 3: queue up the remaining replacements to enter service when
                        # currently busy workers become free.
                        self.pendingreplacements = new_workers

			return json.dumps({"wids": new_worker_ids})

        def registerreleasecallback(self, address):

                with self.lock:
                        if address.startswith("http://"):
                                address = address[7:]
                        self.callback_host, self.callback_address = address.split("/", 1)
                        self.callback_address = "/%s" % self.callback_address

        def lsworkers(self):

                with self.lock:
                        return json.dumps(self.workers, cls=DictEncoder)

        def lsprocs(self):

                with self.lock:
                        return json.dumps(self.procs, cls=DictEncoder)

        def getpids(self):

                with self.lock:
                        return list(self.procs.iterkeys())

        def getresusage(self):
                
                # Get resource usage, as proportions compared to the
                # current hardware spec. The current spec may not have
                # fully taken effect yet, so this really shows what it
                # _will_ be once it does.

                try:
                        cores = int(self.current_hwspec["cores"])
                except KeyError:
                        raise Exception("Must specify hwspec core count before getresusage works")

                try:
                        mem = int(self.current_hwspec["memory"])
                except KeyError:
                        raise Exception("Must specify memory amount before getresusage works")

                tocheck = []

                with self.lock:
                        for proc in self.procs.itervalues():
                                if proc.worker is not None:
                                        tocheck.append(proc)

                now = datetime.datetime.now()

                cpu_usage_samples = []
                mem_usage_samples = []

                # Outside the lock -- processes might end during the check,
                # in which case skip them for stats purposes.
                for proc in tocheck:
                        try:
                                res = proc.getresusage()
                        except Exception as e:
                                print >>sys.stderr, "Skipping process %d (%s)" % (proc.pid, e)

                                continue
                        walltime = (now - proc.start_time).total_seconds() * cores
                        cpu_usage = float(res["cpuseconds"]) / walltime
                        cpu_usage_samples.append(cpu_usage)

                        mem_usage_samples.append(float(res["rsskb"]) / (mem * 1024))

                if len(cpu_usage_samples) == 0:
                        return json.dumps({"cpu": 0, "mem": 0})
                else:
                        return json.dumps({"cpu": sum(cpu_usage_samples) / len(cpu_usage_samples),
                                           "mem": sum(mem_usage_samples) / len(mem_usage_samples)})


class HttpQueue:

        def __init__(self):

                self.queue = Queue.Queue()
                self.should_stop = False
                self.thread = threading.Thread(target=self.work)
                self.thread.start()

        def work(self):

                while True:

                        if self.should_stop:
                                return

                        nextrq = self.queue.get()
                        if nextrq is None:
                                return

                        host, address, params = nextrq
                        
                        hostbits = host.split(":")
                        if len(hostbits) == 1:
                                port = 80
                        else:
                                host = hostbits[0]
                                port = int(hostbits[1])

                        try:

                                params = urllib.urlencode(params)
                                headers = {"Content-type": "application/x-www-form-urlencoded"}
                                conn = httplib.HTTPConnection(host, port, strict=False, timeout=5)
                                conn.request("POST", address, params, headers)
                                response = conn.getresponse()
                                if response.status != 200:
                                        print >>sys.stderr, "Warning: request", host, address, params, "returned status", response.status
                        except Exception as e:
                                
                                print >>sys.stderr, "Exception sending HTTP RPC:", e


        def stop(self):

                self.should_stop = True
                self.queue.put(None)
                self.thread.join()

class TaskPoller:

        def __init__(self, sched):
                self.sched = sched
                self.stop_event = threading.Event()
                self.thread = threading.Thread(target=self.work)
                self.thread.start()

        def work(self):

                while True:

                        for q in sched.queues.itervalues():
                                for pid in q.getpids():
                                        q.pollworkitem(pid)
                                        
                        with q.lock:
                                q.trystartprocs()

                        if self.stop_event.wait(10):
                                return
        
        def stop(self):
                self.stop_event.set()

httpqueue = HttpQueue()
sched = MulticlassScheduler(httpqueue)
poller = TaskPoller(sched)

def thread_stop():
        httpqueue.stop()
        poller.stop()

thread_stop.priority = 10
cherrypy.engine.subscribe("stop", thread_stop)

cherrypy.server.socket_host = '0.0.0.0'

cherrypy.quickstart(sched)

