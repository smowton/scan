#!/usr/bin/python

import cherrypy
import threading
import sys
import json
import copy
import subprocess

gl = threading.Lock()

class Node:

	def __init__(self, address, cores):
		self.address = address
		self.cores = cores
		self.freecores = cores
		self.deleted = False

        def to_dict(self):
                return self.__dict__

class Process:

	def __init__(self, cmd, stdout, stderr, proc, node, cores):
		self.cmd = cmd
                self.stdout = stdout
                self.stderr = stderr
		self.proc = proc
		self.node = node
		self.cores = cores

        def to_dict(self):
                d = copy.deepcopy(self.__dict__)
                if self.proc is not None:
                        d["proc"] = self.proc.pid
                if self.node is not None:
                        d["node"] = self.node.address
                return d
                
procs = dict()
pending = []
nodes = dict()
nextpid = 0

if len(sys.argv) > 1 and sys.argv[1] == "--win":
        runner = ["psexec.exe"]
else:
        runner = ["/usr/bin/ssh"]

class DictEncoder(json.JSONEncoder):

        def default(self, o):
                return o.to_dict()

class TinyScheduler:
 
        def trystartproc(self):

                if len(pending) == 0:
                        return False

                np = pending[0]
                if np.cores is None:
                        goodnodes = sorted(list(nodes.itervalues()), key = lambda x: x.freecores, reverse=True)
                        if len(goodnodes) > 0 and goodnodes[0].freecores > 0:
                                bestnode = goodnodes[0]
                                np.cores = bestnode.freecores
                                np.cmd = np.cmd.replace("%%cores%%", str(np.cores))
                                bestnode.freecores = 0
                        else:
                                bestnode = None
                else:
                        goodnodes = filter(lambda x: x.freecores >= np.cores, nodes.itervalues())
                        if len(goodnodes) > 0:
                                bestnode = goodnodes[0]
                                bestnode.freecores -= np.cores
                        else:
                                bestnode = None

                run = bestnode is not None

                if run:

                        cmdline = runner
                        cmdline.append(bestnode.address)
                        cmdline.append(np.cmd)
                        with open(np.stdout, "w") as so, open(np.stderr, "w") as se:
                                np.proc = subprocess.Popen(cmdline, stdout=so, stderr=se)
                        np.node = bestnode
                        pending.pop(0)

                return run

	def trystartprocs(self):

                while self.trystartproc():
                        pass

	@cherrypy.expose
	def pollproc(self, pid):

		with gl:

			pid = int(pid)
			rp = procs[pid]
			if rp.proc is not None:
				ret = rp.proc.poll()
			else:
				ret = None
			if ret is not None:
				del procs[pid]
				if not rp.node.deleted:
					rp.node.freecores += rp.cores
					self.trystartprocs()
			cherrypy.response.headers['Content-Type'] = "application/json"
			return json.dumps({"pid": pid, "retcode": ret})

	@cherrypy.expose
	def queueproc(self, cmd, stdout, stderr, cores=None):

		with gl:

			if cores is not None:
				cores = int(cores)
			newproc = Process(cmd, stdout, stderr, None, None, cores)
                        global nextpid
			newpid = nextpid
			nextpid += 1
			procs[newpid] = newproc
			pending.append(newproc)
			self.trystartprocs()
			cherrypy.response.headers['Content-Type'] = "application/json"
			return json.dumps({"pid": newpid})

	@cherrypy.expose
	def addnode(self, address, cores):

		with gl:

			cores = int(cores)
			newnode = Node(address, cores)
			nodes[address] = newnode
			self.trystartprocs()
			cherrypy.response.headers['Content-Type'] = "application/json"
			return json.dumps({"status": "ok"})

	@cherrypy.expose
	def delnode(self, address):

		with gl:

			node = nodes[address]
			node.deleted = True
			del nodes[address]
			cherrypy.response.headers['Content-Type'] = "application/json"
			return json.dumps({"status": "ok"})

	@cherrypy.expose
	def modnode(self, address, cores):

		with gl:

			cores = int(cores)
			node = nodes[address]
			change = cores - node.cores
			node.cores += change
			node.freecores += change
			self.trystartprocs()
			cherrypy.response.headers['Content-Type'] = "application/json"
			return json.dumps({"status": "ok"})

        @cherrypy.expose
        def lsnodes(self):

                with gl:
			cherrypy.response.headers['Content-Type'] = "application/json"
                        return json.dumps(nodes, cls=DictEncoder)

        @cherrypy.expose
        def lsprocs(self):

                with gl:
			cherrypy.response.headers['Content-Type'] = "application/json"
                        return json.dumps(procs, cls=DictEncoder)

cherrypy.quickstart(TinyScheduler())
