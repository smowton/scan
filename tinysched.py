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

if len(sys.argv) > 1 and sys.argv[1] == "--win":
        runner = ["psexec.exe"]
else:
        runner = ["/usr/bin/ssh"]

class Worker:

	def __init__(self, address, hwsv, wid):
		self.address = address
                self.hwspec_version = hwsv
                self.wid = wid

        def to_dict(self):
                return self.__dict__

class Process:

	def __init__(self, cmd, pid):
		self.cmd = cmd
                self.pid = pid
		self.proc = None
		self.node = None

        def to_dict(self):
                d = copy.deepcopy(self.__dict__)
                if self.proc is not None:
                        d["proc"] = self.proc.pid
                if self.node is not None:
                        d["node"] = self.node.address
                return d
                
class DictEncoder(json.JSONEncoder):

        def default(self, o):
                return o.to_dict()

class MulticlassScheduler:

        def __init__(self, httpqueue):
                
                # TODO: stop hardcoding classes
                classes = ["A", "B"]
                self.queues = dict()
                for c in classes:
                        self.queues[c] = TinyScheduler(c, httpqueue)

        @cherrypy.expose
        def default(self, callname, classname, **kwargs):
                
                cherrypy.response.headers['Content-Type'] = "application/json"                

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
 
        def __init__(self, classname, httpqueue):
                
                self.procs = dict()
                self.pending = []
                self.nodes = dict()
                self.freenodes = []
                self.pendingreplacements = []
                self.pendingremoves = 0
                self.current_hwspec_version = 0
                self.current_hwspec = dict()
                self.nextpid = 0
                self.nextwid = 0
                self.classname = classname
                self.callback_address = None
                self.callback_host = None
                self.httpqueue = httpqueue
                self.lock = threading.Lock()

        def trystartproc(self):

                if len(self.pending) == 0:
                        return False
                        
                if len(self.freenodes) == 0:
                        return False

                runnode = self.freenodes[-1]
                self.freenodes.pop()

                cmdline = copy.deepcopy(runner)
                cmdline.append(runnode.address)

                np = self.pending[0]

                cmd = np.cmd
                for key, val in self.current_hwspec.iteritems():
                        cmd = cmd.replace("%%%%%s%%%%" % key, val)

                cmdline.append(cmd)
                
                print "Start process", np.pid, cmdline

                np.proc = subprocess.Popen(cmdline)
                np.node = runnode
                self.pending.pop(0)

                return True

	def trystartprocs(self):

                while self.trystartproc():
                        pass

        def release_node(self, freed_node, in_nodes_map = True):

                # Node has already been removed from the free list.
                if in_nodes_map:
                        del self.nodes[freed_node.wid]
                
                if self.callback_host is None:
                        return

                params = {"class": self.classname, "wid": freed_node.wid}
                self.httpqueue.queue.put((self.callback_host, self.callback_address, params))

                print "Release worker callback for", freed_node.wid, freed_node.address

	def pollworkitem(self, pid):

		with self.lock:

			pid = int(pid)
			rp = self.procs[pid]
			if rp.proc is not None:
				ret = rp.proc.poll()
			else:
				ret = None
			if ret is not None:

                                print "Process", pid, "finished"
                                
				del self.procs[pid]
                                freed_node = rp.node

                                if self.pendingremoves > 0:
                                        print "Releasing worker", freed_node.wid, freed_node.address
                                        self.release_node(freed_node)
                                        self.pendingremoves -= 1
                                        freed_node = None
                                elif freed_node.hwspec_version != self.current_hwspec_version:
                                        if len(self.pendingreplacements) == 0:
                                                raise Exception("No pending replacements, but node %s version %d does not match current version %d?" % 
                                                                (freed_node.address, freed_node.hwspec_version, self.current_hwspec_version))
                                        print "Replacing node", freed_node.wid, freed_node.address, "with", self.pendingreplacements[-1].wid, self.pendingreplacements[-1].address
                                        self.release_node(freed_node)
                                        freed_node = self.pendingreplacements[-1]
                                        self.nodes[freed_node.wid] = freed_node
                                        self.pendingreplacements.pop()
                                        
                                if freed_node is not None:
                                        self.freenodes.append(freed_node)
					self.trystartprocs()

			return json.dumps({"pid": pid, "retcode": ret})

        def addworkitem(self, cmd, fsreservation, dbreservation):

		with self.lock:

			newpid = self.nextpid
			newproc = Process(cmd, newpid)
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

	def addnode(self, address):

		with self.lock:

                        newnode = self.newworker(address)
                        self.nodes[newnode.wid] = newnode
                        self.freenodes.append(newnode)
			self.trystartprocs()
			return json.dumps({"wid": newnode.wid})

	def delnode(self):

		with self.lock:

                        if len(self.nodes) == 0:
                                raise Exception("No workers in this class pool")
                        if len(self.freenodes) == 0:
                                self.pendingremoves += 1
                        else:
                                todel = self.freenodes[-1]
                                self.freenodes.pop()
                                self.release_node(todel)
			return json.dumps({"status": "ok"})

	def modhwspec(self, addresses, newspec):

		with self.lock:

                        addresses = [x.strip() for x in addresses.split(",") if len(x.strip()) > 0]
                        if len(addresses) != len(self.nodes):
                                raise Exception("Must supply the same number of replacement workers as are currently in the pool (supplied %d, have %d)" % (len(addresses), len(self.nodes)))
                                
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
                                self.release_node(w, False)

                        # Step 2: immediately replace any free workers.
                        freenodes = self.freenodes
                        self.freenodes = []
                        for w in freenodes:
                                self.release_node(w)
                                replace_with = new_workers[-1]
                                new_workers.pop()
                                self.nodes[replace_with.wid] = replace_with
                                self.freenodes.append(replace_with)

                        print "Replaced", len(self.nodes) - len(new_workers), "workers immediately;", len(new_workers), "pending"

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

        def lsnodes(self):

                with self.lock:
                        return json.dumps(self.nodes, cls=DictEncoder)

        def lsprocs(self):

                with self.lock:
                        return json.dumps(self.procs, cls=DictEncoder)

        def getpids(self):

                with self.lock:
                        return list(self.procs.iterkeys())

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

class ProcessPoller:

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
poller = ProcessPoller(sched)

def thread_stop():
        httpqueue.stop()
        poller.stop()

thread_stop.priority = 10
cherrypy.engine.subscribe("stop", thread_stop)

cherrypy.quickstart(sched)

