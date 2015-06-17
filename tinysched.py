#!/usr/bin/env python

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
import websupport
import imp
import math

import scan_templates
import integ_analysis.results

runner = ["/usr/bin/ssh", "-o", "StrictHostKeyChecking=no"]

def reward_loss(reward_scale, delay):
	return reward_scale[1] * delay

def est_reward(estsize, size_time, thread_time, reward_scale, cores):

	multi_thread_time = est_time(estsize, size_time, thread_time, cores)
	return reward_scale[1] * (reward_scale[0] - multi_thread_time)

def est_time(estsize, size_time, thread_time, cores):
	
	single_thread_time = size_time[0] + (size_time[1] * estsize)
	return ((1 - thread_time) + (thread_time / cores)) * single_thread_time

def core_choices(max):

	choices = [max]
	# Round down to power of 2:
	lg = math.log(max, 2)
	flg = math.floor(lg)
	if flg == lg:
		flg = lg - 1
	cores = int(math.pow(2, flg))
	while cores >= 1:
		choices.append(cores)
		cores /= 2
	return choices

def print_dt(dt):

	if dt.date() == datetime.date.today():
		return dt.strftime("%H:%M:%S")
	else:
		return dt.strftime("%Y-%m-%d %H:%M:%S")

class Worker:

	def __init__(self, address, wid, totalcores, totalmemory):
		self.address = address
                self.wid = wid
		self.cores = totalcores
		self.memory = totalmemory
		self.free_cores = totalcores
		self.free_memory = totalmemory
		self.running_processes = []
                self.delete_pending = False
                self.delete_pending_callback = None

        def to_dict(self):
                return self.__dict__

	def fully_occupied(self):
		return self.free_cores == 0

class Task:

	def __init__(self, cmd, pid, classname, maxcores, mempercore, estsize, sched):
		self.cmd = cmd
                self.pid = pid
		self.maxcores = maxcores
		self.mempercore = mempercore
		self.estsize = estsize
		self.proc = None
		self.worker = None
                self.start_time = None
                self.sched = sched
		self.classname = classname
		self.failures = 0
		self.run_attributes = None

        def to_dict(self):
                d = copy.copy(self.__dict__)
                if self.proc is not None:
                        d["proc"] = self.proc.pid
                if self.worker is not None:
                        d["worker"] = self.worker.address
                del d["sched"]
                if self.start_time is not None:
                        d["start_time"] = self.start_time.isoformat()
                return d

        def taskdir(self):
                return "/tmp/scantask/%d" % self.pid

        def pidfile(self):
                return "%s/pid" % self.taskdir()

        def workcountfile(self):
                return "%s/workcount" % self.taskdir()

        def getresusage(self):
                cmd = self.sched.getrunner(self.worker)
                cmd.append(self.resource_script_path)
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

        def mktemplatecombo(self):
                return websupport.mkcombo("template", [{"name": k, "desc": v["desc"]} for (k, v) in scan_templates.templates().iteritems()])
        
        def mkclasscombo(self):
                return websupport.mkcombo("classname", [{"name": key, "desc": vals["description"]} for key, vals in self.classes.iteritems()])

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
<p>Max cores: <input name="maxcores"/></p>
<p>Memory per core: <input name="mempercore"/></p>
<p>Estimated job size: <input name="estsize"/></p>
</td></tr>
<tr><td><input type="submit"/></td></tr></table></p>
</form></body></html>""" % (self.mktemplatecombo(), self.mkclasscombo())

class MulticlassScheduler:

        def __init__(self, httpqueue, classfile, classargs):

		self.resource_script_path = "/home/user/csmowton/scan/getres.py"
		self.worker_login_username = "csmowton"

                if classfile is not None:
                        self.classes = imp.load_source("user_classes_module", classfile).getclasses(**classargs)
                else:
			self.classes = {"linux": {"lastwph": 0, "description": "Generic Linux tasks", "time_reward": None, "size_time": None, "thread_time": None} }

		self.taskqueue = []
		self.httpqueue = httpqueue

                self.ui = SubmitUI(self.classes)
                self.results = integ_analysis.results.ResultViewer()

                self.procs = dict()
                self.pending = []
                self.workers = dict()
                self.nextpid = 0
                self.nextwid = 0
                self.callback_address = None
                self.callback_host = None
                self.httpqueue = httpqueue
                self.lock = threading.Lock()

		self.scale_reward_loss = 0.0
		self.queue_reward_loss = 0.0

		# Adaptive scale selection parameters:
		self.new_worker_wait_time = 0.1 # Unit: hours
		self.worker_pool_size_threshold = 100
		self.worker_pool_oversize_penalty = 10

        @cherrypy.expose
        def addworkitem_ui(self, template, templateinputs, classname, script, maxcores, mempercore, estsize):

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
                                ret = self.default(callname="addworkitem", classname=classname, cmd=s, set_ct=False, maxcores = maxcores, mempercore = mempercore, estsize = estsize)
                                pids.append(json.loads(ret)["pid"])
                              
                        return "<html><body><p>Created PIDs %s</p></body></html>" % ", ".join([str(p) for p in pids])

                except Exception as e:

                        return "<html><body><p>Error: %s</p><p>%s</p></body></html>" % (e, traceback.format_exc(e).replace("\n", "<br/>"))

        @cherrypy.expose
        def default(self, callname, **kwargs):
                
                if "set_ct" not in kwargs:
                        cherrypy.response.headers['Content-Type'] = "application/json"
                else:
                        del kwargs["set_ct"]

		if "classname" in kwargs and kwargs["classname"] not in self.classes:
                        return json.dumps({"error": "No such class %s" % classname})
		
                try:
                        call = getattr(self, callname)
                except AttributeError:
                        return json.dumps({"error": "No such method %s" % callname})

                return call(**kwargs)

        @cherrypy.expose
        def getclasses(self):
                return json.dumps(self.classes.keys())

        @cherrypy.expose
        def ping(self, echo):
                return json.dumps({"echo": echo})

        def getrunner(self, worker):
                cmdline = copy.deepcopy(runner)
                cmdline.append("%s@%s" % (self.worker_login_username, worker.address))
                return cmdline

	# Should return attributes_dict, best_worker                
	def select_run_attributes(self, proc):

		will_use_cores = None
		best_worker = None

		report = []

		reward_scale = self.classes[proc.classname]["time_reward"]
		if reward_scale is None:
			# No reward scale given; try to grab as many cores as the process type can use
			report.append("Class " + proc.classname + " gives no profiling parameters: try for " + str(proc.maxcores) + " cores")
			maxcores = proc.maxcores
		else:
			size_time = self.classes[proc.classname]["size_time"]
			thread_time = self.classes[proc.classname]["thread_time"]
			assert(size_time is not None and thread_time is not None)

			nthreads_choices = core_choices(proc.maxcores)
			reward_choices = {c: est_reward(proc.estsize, size_time, thread_time, reward_scale, c) for c in nthreads_choices}

			report.append("Estimated core -> rewards " + ", ".join("%d -> %g" % x for x in reward_choices.iteritems()))
		
			slots_available = {c: 0 for c in nthreads_choices}

			for w in self.workers.itervalues():

				if w.delete_pending or w.fully_occupied():
					continue

				this_w_cores = min(w.free_cores, w.free_memory / proc.mempercore)
				for c in nthreads_choices:
					slots_available[c] += (this_w_cores / c)

			# Approximation: assume the current set of running processes represents the expected rate of cores
			# becoming free again.
					
			report.append("Slots available " + ", ".join("%d -> %d" % x for x in slots_available.iteritems()))
			
			running_procs = []
			for w in self.workers.itervalues():
				running_procs.extend(w.running_processes)

			allocated_cores = sum(p.run_attributes["cores"] for p in running_procs)
			finish_times = sorted(p.expected_finish_time for p in running_procs)
			
			if allocated_cores > 0 and len(finish_times) > 1:
				finish_rate = allocated_cores / (finish_times[-1] - finish_times[0]).total_seconds()
			else:
				finish_rate = float(1) / 3600 # Shrug! One every hour?

			report.append("Expected finish rate: " + str(finish_rate * (60 * 60)) + " cores/hr")

			for (cores, slots) in slots_available.iteritems():

				# Approximation: assuming the other processes in the queue
				# have similar reward tradeoffs as this one.
				if slots < len(self.pending):

					report.append("Resource shortage if queued processes use " + str(cores) + (" cores (%d in queue; %d slots available)" % (len(self.pending), slots)))

					penalised_procs = len(self.pending) - slots
					slot_recovery_rate = (finish_rate * (60 * 60)) / cores

					# We could wait for workers to become free...
					loss_per_proc_waiting = reward_loss(reward_scale, slot_recovery_rate * (float(penalised_procs) / 2))
					total_loss_waiting = loss_per_proc_waiting * penalised_procs

					report.append("Reward loss due to waiting: " + str(total_loss_waiting) + " (slots become free every " + str(slot_recovery_rate) + " hours)")
				
					# Alternatively we could hire to get out of trouble. Factor in assumed lag:
					loss_per_proc_hiring = reward_loss(reward_scale, self.new_worker_wait_time)
					total_loss_hiring = loss_per_proc_hiring * penalised_procs

					report.append("Reward loss due to hiring: " + str(total_loss_hiring) + " (hiring new workers takes " + str(self.new_worker_wait_time) + " hours)")

					total_cores_now = sum(w.cores for w in self.workers.itervalues())
					excess_cores = (total_cores_now + (penalised_procs * cores)) - self.worker_pool_size_threshold
					if excess_cores > 0:
						
						extra_penalty = (self.worker_pool_oversize_penalty * excess_cores)
						report.append("Extra penalty due to hiring " + str(excess_cores) + " excess cores: " + str(extra_penalty))
						total_loss_hiring += extra_penalty

					reward_choices[cores] -= min(total_loss_hiring, total_loss_waiting)
					
			maxcores, ignored = max(reward_choices.iteritems(), key = lambda x: x[1])
			report.append("Setting max cores to assign: " + str(maxcores))

		for w in self.workers.itervalues():

			if w.delete_pending or w.fully_occupied():
				continue

			this_w_cores = min(maxcores, w.free_cores, w.free_memory / proc.mempercore)
			if best_worker is None or this_w_cores > will_use_cores:
				best_worker = w
				will_use_cores = this_w_cores
				if will_use_cores == maxcores:
					break

		report.append("Selected worker " + w.address + " / " + str(w.wid) + " with " + str(will_use_cores) + " cores")

		if reward_scale is not None and best_worker is not None:
			
			evaluated_cores = sorted(reward_choices.iterkeys())
			for c1, c2 in zip(evaluated_cores[:-1], evaluated_cores[1:]):

				for i in range(c1 + 1, c2):

					c1_prop = float(i - c1) / float(c2 - c1)
					c2_prop = 1 - c1_prop
					reward_choices[i] = (c1_prop * reward_choices[c1]) + (c2_prop * reward_choices[c2])

			# Loss incurred for running on a smaller worker than we'd like.
			scale_reward_loss = max(reward_choices.itervalues()) - reward_choices[will_use_cores]
			est_finish_time = est_time(proc.estsize, size_time, thread_time, will_use_cores)

			report.append("Loss incurred due to available slot scale: " + str(scale_reward_loss))

			# Loss incurred for queueing:
			queue_reward_loss = reward_loss(reward_scale, (datetime.datetime.now() - proc.queue_time).total_seconds() / (60 * 60))

			report.append("Loss incurred due to queueing: " + str(queue_reward_loss))

			report.append("Estimated finish time: " + print_dt(datetime.datetime.now() + datetime.timedelta(seconds = est_finish_time)))

		else:

			scale_reward_loss = 0
			queue_reward_loss = 0
			est_finish_time = datetime.datetime.now() + datetime.timedelta(hours=1) # Shrug!

		if best_worker is None:
			run_attr = None
		else:
			run_attr = {"cores": will_use_cores, "memory": proc.mempercore * will_use_cores}

		if best_worker is not None:
			print "\n".join(report)

		return run_attr, best_worker, scale_reward_loss, queue_reward_loss, est_finish_time

	def update_worker_resource_stats(self, worker):

		idle_cores_prop = str(float(worker.free_cores) / worker.cores)
		idle_mem_prop = str(float(worker.free_memory) / worker.memory)

		attrs = [(idle_cores_prop, "idle_cores"),
			 (idle_mem_prop, "idle_mem")]

		commands = ["echo %s > /tmp/scan_%s.new; mv /tmp/scan_%s.new /tmp/scan_%s" % (val, key, key, key) for (val, key) in attrs]

		composite_command = "; ".join(commands)
		
		cmdline = self.getrunner(worker)
		cmdline.append(composite_command)
		subprocess.check_call(cmdline)

        def trystartproc(self):

                if len(self.pending) == 0:
                        return False
                        
                np = self.pending[0]

		run_attributes, run_worker, scale_reward_loss, queue_reward_loss, est_finish_time = self.select_run_attributes(np)
		if run_worker is None:
			return False

		np.run_attributes = run_attributes
		np.expected_finish_time = est_finish_time

                cmdline = self.getrunner(run_worker)

                # Expose the current environment:
                cmd = np.cmd
                for key, val in run_attributes.iteritems():
                        cmd = cmd.replace("%%%%%s%%%%" % key, str(val))
                        cmd = "export SCAN_%s=%s; %s" % (key.upper(), val, cmd)

                # Prepend bookkeeping:
                cmd = "mkdir -p %s; export SCAN_WORKCOUNT_FILE=%s; echo $$ > %s; %s" % (np.taskdir(), np.workcountfile(), np.pidfile(), cmd)

                cmdline.append(cmd)

                print "Start process", np.pid, cmdline

                np.proc = subprocess.Popen(cmdline)
                np.worker = run_worker
		run_worker.free_cores -= run_attributes["cores"]
		run_worker.free_memory -= run_attributes["memory"]
		run_worker.running_processes.append(np)
                np.start_time = datetime.datetime.now()
                self.pending.pop(0)

		self.update_worker_resource_stats(np.worker)

		# Accounting: reward lost due to queueing and suboptimal worker assignment:
		self.scale_reward_loss += scale_reward_loss
		self.queue_reward_loss += queue_reward_loss

                return True

	def trystartprocs(self):

                while self.trystartproc():
                        pass

        def release_worker(self, freed_worker, callback, in_workers_map = True):

                # Worker has already been removed from the free list.
                if in_workers_map:
                        del self.workers[freed_worker.wid]
                
                params = {"wid": freed_worker.wid, "address": freed_worker.address}

                host, address = None, None

                if callback is not None:
                        host, address = callback
                elif self.callback_host is not None:
                        host, address = self.callback_host, self.callback_address

                if host is None:
                        return

                self.httpqueue.queue.put((host, address, params))
                print "Release worker callback queued for", freed_worker.wid, freed_worker.address

	def pollworkitem(self, pid):

		with self.lock:

			pid = int(pid)
			rp = self.procs[pid]
			if rp.proc is not None:
				ret = rp.proc.poll()
			else:
				ret = None
			if ret is not None:

				if ret == 0:
	                                print "Task", pid, "finished"
				else:
					print "*** Task", pid, "failed! (rc: %d)" % ret
                        
                                freed_worker = rp.worker
                                freed_worker.free_cores += rp.run_attributes["cores"]
				freed_worker.free_memory += rp.run_attributes["memory"]
				freed_worker.running_processes.remove(rp)

				self.update_worker_resource_stats(freed_worker)

				if ret == 0:

	                                try:
        	                                read_proc = self.getrunner(rp.worker)
                	                        read_proc.append("cat %s" % rp.workcountfile())
                        	                workdone = int(subprocess.check_output(read_proc))
                                	        if workdone == 0:
                                        	        workdone = 1
	                                except subprocess.CalledProcessError:
        	                                print "Can't read from", rp.workcountfile(), "assuming one unit of work completed"
                	                        workdone = 1
	                                except ValueError:
        	                                print "Junk in", rp.workcountfile(), "assuming one unit of work completed"
                	                        workdone = 1

	                                runhours = (datetime.datetime.now() - rp.start_time).total_seconds() / (60 * 60)
					newwph = float(workdone) / runhours
	                                self.classes[rp.classname]["lastwph"] = newwph
	                                print "Task completed %g units of work in %g hours; new wph = %g" % (workdone, runhours, newwph)
        
					del self.procs[pid]

				else:

					if rp.failures == 10:
						print "Too many failures; giving up"
						del self.procs[pid]
					else:
						rp.failures += 1
						print "Queueing for retry", rp.failures
						rp.worker = None
						rp.proc = None
						rp.queue_time = datetime.datetime.now()
						self.pending.append(rp)

                                if freed_worker.delete_pending:
					if len(freed_worker.running_processes) == 0:
						print "Releasing worker", freed_worker.wid, freed_worker.address
						self.release_worker(freed_worker, freed_worker.delete_pending_callback)
                                        freed_worker = None

                                if freed_worker is not None:
					self.trystartprocs()

			return json.dumps({"pid": pid, "retcode": ret})

        def addworkitem(self, cmd, classname, maxcores, mempercore, estsize):

		mempercore = int(mempercore)
		maxcores = int(maxcores)
		estsize = float(estsize)

		with self.lock:

			newpid = self.nextpid
			newproc = Task(cmd, newpid, classname, maxcores, mempercore, estsize, self)
			self.nextpid += 1
			self.procs[newpid] = newproc
			newproc.queue_time = datetime.datetime.now()
			self.pending.append(newproc)
                        print "Queue work item", newpid, cmd
			self.trystartprocs()
			return json.dumps({"pid": newpid})

        def newworker(self, address, cores, memory):
                
                wid = self.nextwid
                self.nextwid += 1
                print "Add worker", wid, address
                return Worker(address, wid, cores, memory)

	def addworker(self, address, cores, memory):
		
		cores = int(cores)
		memory = int(memory)

		with self.lock:

                        newworker = self.newworker(address, cores, memory)
                        self.workers[newworker.wid] = newworker
			self.trystartprocs()
			return json.dumps({"wid": newworker.wid})

	def delworker(self, callbackaddress=None, wid=None):

		with self.lock:

                        if callbackaddress is not None:
                                callbackaddress = self.parsecallbackaddress(callbackaddress)

                        if wid is not None:
                                wid = int(wid)

                        if len(self.workers) == 0:
                                raise Exception("No workers registered")

                        if wid is not None:

                                try:
                                        target = self.workers[wid]
                                except KeyError:
                                        raise Exception("No such worker %d in pool" % wid)
                                if target.delete_pending:
                                        raise Exception("Worker %d already pending deletion" % wid)

                                if len(target.running_processes) == 0:
                                        self.release_worker(target, callbackaddress)
                                else:
                                        target.delete_pending = True
                                        target.delete_pending_callback = callbackaddress

                        else:

                                target = None
				for w in self.workers:
					if len(w.running_processes) == 0:
						target = w
						break
				if target is None:
					target = min(self.workers, key = lambda w : min(p.expected_finish_time for p in w.running_processes))
                                        target.delete_pending = True
					target.delete_pending_callback = callbackaddress
                                else:
                                        self.release_worker(target, callbackaddress)

			return json.dumps({"status": "ok"})

        def parsecallbackaddress(self, address):
                if address.startswith("http://"):
                        address = address[7:]
                host, address = address.split("/", 1)
                address = "/%s" % address
                return host, address

        def registerreleasecallback(self, address):

                with self.lock:
                        self.callback_host, self.callback_address = self.parsecallbackaddress(address)

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
                
                with self.lock:
			tocheck = [p for p in self.procs.itervalues() if p.worker is not None]

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

                        walltime = (now - proc.start_time).total_seconds()
                        cpu_usage = float(res["cpuseconds"]) / walltime
                        cpu_usage_samples.append(cpu_usage)

                        mem_usage_samples.append(float(res["rsskb"]) / proc.worker.memory)

                if len(cpu_usage_samples) == 0:
                        return json.dumps({"cpu": 0, "mem": 0})
                else:
                        return json.dumps({"cpu": sum(cpu_usage_samples) / len(cpu_usage_samples),
                                           "mem": sum(mem_usage_samples) / len(mem_usage_samples)})

        def getwph(self, classname):
                return str(self.classes[classname]["lastwph"])

	def getqueuerewardloss(self):
		return str(self.queue_reward_loss)

	def getscalerewardloss(self):
		return str(self.scale_reward_loss)

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

			for pid in sched.getpids():
				sched.pollworkitem(pid)
                                        
                        with sched.lock:
                                sched.trystartprocs()

                        if self.stop_event.wait(10):
                                return
        
        def stop(self):
                self.stop_event.set()

classfile = None
classargs = {}

if len(sys.argv) >= 2:
        classfile = sys.argv[1]
        for arg in sys.argv[2:]:
                bits = [a.strip() for a in arg.split("=", 1)]
                if len(bits) != 2:
                        print >>sys.stderr, "Bad argument", arg, "should have form key=value"
                        sys.exit(1)
                classargs[bits[0]] = bits[1]

httpqueue = HttpQueue()

sched = MulticlassScheduler(httpqueue, classfile, classargs)
poller = TaskPoller(sched)

def thread_stop():
        httpqueue.stop()
        poller.stop()

thread_stop.priority = 10
cherrypy.engine.subscribe("stop", thread_stop)

cherrypy.server.socket_host = '0.0.0.0'
cherrypy.server.thread_pool = 100

cherrypy.quickstart(sched)

