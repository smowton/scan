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
import os.path
import random
import numpy
import os
import shutil

import scan_templates
import integ_analysis.results

runner = ["/usr/bin/ssh", "-o", "StrictHostKeyChecking=no"]
copier = ["/usr/bin/scp", "-o", "StrictHostKeyChecking=no"]
scanfs_root = "/mnt/scanfs"

def reward_loss(reward_scale, delay):
	return reward_scale[1] * delay

# Time units: hours
def actual_reward(reward_scale, actual_time):
	return reward_scale[1] * (reward_scale[0] - actual_time)

def est_reward(estsize, size_time, thread_time, reward_scale, cores):
	multi_thread_time = est_time(estsize, size_time, thread_time, cores)
	return actual_reward(reward_scale, multi_thread_time)

def est_time(estsize, size_time, thread_time, cores):
	
	single_thread_time = size_time[0] + (size_time[1] * estsize)
	return ((1 - thread_time) + (thread_time / cores)) * single_thread_time

def inverse_est_time(mctime, cores, thread_time):
	return mctime / ((1 - thread_time) + (thread_time / cores))

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

def simple_linreg(xs, ys):

	xtran = numpy.vstack([xs, numpy.ones(len(xs))]).T
	return numpy.linalg.lstsq(xtran, ys)[0]

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

	def __init__(self, cmd, pid, classname, maxcores, mempercore, estsize, filesin, filesout, sched):
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
		self.filesin = filesin
		self.filesout = filesout

        def to_dict(self):
                d = copy.copy(self.__dict__)
                if self.proc is not None:
                        d["proc"] = self.proc.pid
                if self.worker is not None:
                        d["worker"] = self.worker.address
                del d["sched"]
                if self.start_time is not None:
                        d["start_time"] = self.start_time.isoformat()
		if "queue_time" in d and d["queue_time"] is not None:
			d["queue_time"] = d["queue_time"].isoformat()
		if "expected_finish_time" in d and d["expected_finish_time"] is not None:
			d["expected_finish_time"] = d["expected_finish_time"].isoformat()
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
<p>Files required (colon-separated): <input name="filesin"/></p>
<p>Files produced (colon-separated): <input name="filesout"/></p>
</td></tr>
<tr><td><input type="submit"/></td></tr></table></p>
</form></body></html>""" % (self.mktemplatecombo(), self.mkclasscombo())

class MulticlassScheduler:

	@cherrypy.config(**{'response.timeout': 3600})

        def __init__(self, httpqueue, classfile, classargs):

		self.resource_script_path = "/home/user/csmowton/scan/getres.py"
		self.worker_login_username = "user"

                if classfile is not None:
                        self.classes = imp.load_source("user_classes_module", classfile).getclasses(**classargs)
                else:
			self.classes = {"linux": {"lastwph": 0, "description": "Generic Linux tasks", "time_reward": None, "size_time": None, "thread_time": None} }

		for v in self.classes.itervalues():
			v["time_history"] = []

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
		self.total_reward = 0.0

		# Adaptive scale selection parameters:
		self.new_worker_wait_time = 0.1 # Unit: hours
		self.worker_pool_size_threshold = 100
		self.worker_pool_oversize_penalty = 10
		self.worker_pool_size_dynamic = False

		self.dfs_map = dict()

        @cherrypy.expose
        def addworkitem_ui(self, template, templateinputs, classname, script, maxcores, mempercore, estsize, filesin, filesout):

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
                                ret = self.default(callname="addworkitem", classname=classname, cmd=s, set_ct=False, maxcores = maxcores, mempercore = mempercore, estsize = estsize, filesin = filesin, filesout = filesout)
                                pids.append(json.loads(ret)["pid"])
                              
                        return "<html><body><p>Created PIDs %s</p></body></html>" % ", ".join([str(p) for p in pids])

                except Exception as e:

                        return "<html><body><p>Error: %s</p><p>%s</p></body></html>" % (e, traceback.format_exc(e).replace("\n", "<br/>"))

        @cherrypy.expose
        def default(self, callname, **kwargs):
                
		if callname == "dfsget":
			pass
		elif "set_ct" not in kwargs:
                        cherrypy.response.headers['Content-Type'] = "application/json"
                else:
                        del kwargs["set_ct"]

		if "classname" in kwargs and kwargs["classname"] not in self.classes:
                        return json.dumps({"error": "No such class %s" % kwargs["classname"]})
		
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

	def getcopier(self, worker, fromfiles, topath, copyout = True, extra_args = []):
		cmdline = copy.deepcopy(copier)
		cmdline.extend(extra_args)
		if copyout:
			cmdline.extend(fromfiles)
			cmdline.append("%s@%s:%s" % (self.worker_login_username, worker.address, topath))
		else:
			cmdline.extend(["%s@%s:%s" % (self.worker_login_username, worker.address, f) for f in fromfiles])
			cmdline.append(topath)
		return cmdline

	def getdfscopier(self, worker, fromfiles, topath, copyout = True):
		return self.getcopier(worker, fromfiles, topath, copyout = copyout, extra_args = ["-i", "/home/%s/.ssh/scanfs_key" % self.worker_login_username])

        def getrunner(self, worker):
                cmdline = copy.deepcopy(runner)
                cmdline.append("%s@%s" % (self.worker_login_username, worker.address))
                return cmdline

	# Should return attributes_dict, best_worker                
	def select_run_attributes(self, proc):

		will_use_cores = None
		missing_files = None
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
			
			if allocated_cores > 0 and len(finish_times) > 1 and finish_times[-1] != finish_times[0]:
				finish_rate = allocated_cores / (finish_times[-1] - finish_times[0]).total_seconds()
			elif len(finish_times) == 1:
				finish_rate = allocated_cores / max((finish_times[0] - datetime.datetime.now()).total_seconds(), 1)
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
					loss_per_proc_waiting = reward_loss(reward_scale, (1 / slot_recovery_rate) * (float(penalised_procs) / 2))
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
						if self.worker_pool_size_dynamic:
							# Since we're evidently pushing on the limit, try raising it and see if
							# we get a pressure notification in response.
							self.worker_pool_size_threshold += 1
							report.append("Incremented worker pool size limit, now %d" % self.worker_pool_size_threshold)

					reward_choices[cores] -= min(total_loss_hiring, total_loss_waiting)
					
			maxcores, ignored = max(reward_choices.iteritems(), key = lambda x: x[1])
			report.append("Setting max cores to assign: " + str(maxcores))

		for w in self.workers.itervalues():

			if w.delete_pending or w.fully_occupied():
				continue

			def score_file(f, wid):
				if f not in self.dfs_map:
					# Error will be resolved later
					return 0
				stat = self.dfs_map[f]
				if wid in stat["pending"]:
					return 0.5
				elif wid not in stat["complete"]:
					return 1.0
				return 0

			this_w_cores = min(maxcores, w.free_cores, w.free_memory / proc.mempercore)
			this_w_missing_files = sum(score_file(f, w.wid) for f in proc.filesin)

			def missing_files_better(oldval, newval):
				if os.getenv("SCAN_DFS_TEST") is not None:
					return newval > oldval
				else:
					return newval < oldval
			
			if best_worker is None or this_w_cores > will_use_cores or (this_w_cores == will_use_cores and missing_files_better(missing_files, this_w_missing_files)):
				best_worker = w
				will_use_cores = this_w_cores
				missing_files = this_w_missing_files
				if will_use_cores == maxcores and missing_files == 0:
					break

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

			report.append("Estimated duration: " + str(datetime.timedelta(hours = est_finish_time)) + "; estimated finish time: " + print_dt(datetime.datetime.now() + datetime.timedelta(hours = est_finish_time)))

		else:

			scale_reward_loss = 0
			queue_reward_loss = 0
			est_finish_time = 1 # Shrug!

		if best_worker is None:
			run_attr = None
		else:

			history = self.classes[proc.classname]["time_history"]
			if len(history) > 3 and all([cores == will_use_cores for (size, cores, tm) in history]):
				changed = False
				if will_use_cores > 1:
					will_use_cores /= 2
					changed = True
				elif will_use_cores == 1 and slots_available[2] != 0:
					will_use_cores = 2
					changed = True
				if changed:
					print >>sys.stderr, "Changed core count in order to measure scalability"

			report.append("Selected worker " + best_worker.address + " / " + str(best_worker.wid) + " with " + str(will_use_cores) + " cores and " + str(missing_files) + " missing files.")
			run_attr = {"cores": will_use_cores, "memory": proc.mempercore * will_use_cores}

		if best_worker is not None:
			print "\n".join(report)

		return run_attr, best_worker, scale_reward_loss, queue_reward_loss, datetime.datetime.now() + datetime.timedelta(hours = est_finish_time)

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

	def abs_dfs_file(self, relpath):
		return os.path.join(scanfs_root, relpath)

	def abs_done_file(self, relpath):
		path, basename = os.path.split(relpath)
		return os.path.join(scanfs_root, path, ".%s.done" % basename)

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

		# Prepend file fetching (TODO: make this more asynchronous)
		for needf in np.filesin:
			try:
				dfsstat = self.dfs_map[needf]
			except KeyError:
				print >>sys.stderr, "Process wants file %s which is not in the SCAN DFS" % needf
				continue
			if run_worker.wid in dfsstat["complete"]:
				print "Required file", needf, "already present"
				pass
			elif run_worker.wid in dfsstat["pending"]:
				print "Required file", needf, "pending"
				await_donefile = self.abs_done_file(needf)
				cmd = "while [ ! -f %s ]; do echo Waiting for %s; sleep 5; done; %s" % (await_donefile, await_donefile, cmd)
			else:
				# Neither complete nor pending.
				abs_local_path = self.abs_dfs_file(needf)
				create_donefile = self.abs_done_file(needf)
				# Pick some worker that already has the file:
				copy_from = self.workers[random.choice(dfsstat["complete"])]
				copier = self.getdfscopier(copy_from, [abs_local_path], abs_local_path, copyout = False)
				cmd = "mkdir -p %s; %s || exit 1; touch %s; %s" % (os.path.split(abs_local_path)[0], " ".join(copier), create_donefile, cmd)
				dfsstat["pending"].append(run_worker.wid)
				print "Required file", needf, "will be copied from", copy_from.address

		# Prepend output directory creation
		cmd = "%s; %s" % ("; ".join(["mkdir -p %s" % os.path.dirname(self.abs_dfs_file(x)) for x in np.filesout]), cmd)

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

                if in_workers_map:
                        del self.workers[freed_worker.wid]

		# Copy out any files that only this worker has available
		for f, stat in self.dfs_map.iteritems():
			if freed_worker.wid in stat["pending"]:
				print "Released worker", freed_worker.wid, "still marked pending", f
				stat["pending"].remove(freed_worker.wid)
			if freed_worker.wid in stat["complete"]:
				if len(stat["complete"]) == 1:
					try:
						give_to_worker = random.choice([v for v in self.workers.values() if not v.delete_pending])
					except IndexError:
						print "No workers remaining! The DFS loses the file"
						continue
					print "Only removed worker has file %s; giving to %s / %s" % (f, give_to_worker.address, give_to_worker.wid)
					copier = self.getdfscopier(give_to_worker, [self.abs_dfs_file(f)], self.abs_dfs_file(f))
					runner = self.getrunner(freed_worker)
					runner.append(copier)
					try:
						subprocess.check_call(runner)
						stat["complete"] = [give_to_worker.wid]
					except subprocess.CalledProcessError:
						print "Copy failed! The DFS loses file", f
                
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

	def update_class_model(self, classname):

		cl = self.classes[classname]

		if cl["size_time"] is None:
			return

		history_limit = 100
		history_needed = 5
		if len(cl["time_history"]) > history_limit:
			cl["time_history"] = cl["time_history"][1:]

		# Step 1: Guess how much the estsize parameter is telling us using the most popular core count
		
		history = cl["time_history"]

		core_records = dict()
		for (size, cores, td) in history:
			if cores not in core_records:
				core_records[cores] = 0
			core_records[cores] += 1

		select_cores = max(core_records.iterkeys(), key = lambda x : core_records[x])
		model_records = [(size, td) for (size, cores, td) in history if cores == select_cores]

		if len(model_records) < history_needed:
			return

		# Use the existing thread model to infer single-core times from multi-core ones.
		sizes = [x for (x, y) in model_records]
		hours = [inverse_est_time(y.total_seconds() / (60 * 60), select_cores, self.classes[classname]["thread_time"]) for (x, y) in model_records]

		gradient, icpt = simple_linreg(sizes, hours)
		print "Updated model for class %s: time = %f(estsize) + %f" % (classname, gradient, icpt)
		self.classes[classname]["size_time"] = (icpt, gradient)

		similar_size_tolerance = 0.05

		thread_reductions_observed = []

		for (size, cores, td) in history:

			similar_samples = [x for x in history if x[0] <= size * (1 + similar_size_tolerance) and x[0] >= size * (1 - similar_size_tolerance)]
			cores_represented = len(set([x[1] for x in similar_samples]))
			if cores_represented < 2:
				continue
			
			sum_by_cores = dict()
			for (s, c, td) in similar_samples:
				if c not in sum_by_cores:
					sum_by_cores[c] = []
				sum_by_cores[c].append(td.total_seconds() / (60 * 60))

			for c in sum_by_cores.keys():
				sum_by_cores[c] = sum(sum_by_cores[c]) / len(sum_by_cores[c])

			least_cores = min(sum_by_cores.iterkeys())
			least_cores_avg = sum_by_cores[least_cores]

			ideal_props = [float(least_cores) / float(c) for c in sum_by_cores.iterkeys()]
			actual_props = [sum_by_cores[c] / least_cores_avg for c in sum_by_cores.iterkeys()]

			gradient, icpt = simple_linreg(ideal_props, actual_props)

			thread_reductions_observed.append(1.0 - icpt)

		if len(thread_reductions_observed) != 0:

			thread_reductions_observed = sorted(thread_reductions_observed)
			new_prop = max(0.0, min(thread_reductions_observed[len(thread_reductions_observed) / 2], 1.0))
			self.classes[classname]["thread_time"] = new_prop

			print "Updated thread model for class %s: %f scalable" % (classname, new_prop) 

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

				if ret == 0:
					self.classes[rp.classname]["time_history"].append((rp.estsize, rp.run_attributes["cores"], datetime.datetime.now() - rp.start_time))
					self.update_class_model(rp.classname)

				self.update_worker_resource_stats(freed_worker)

				if ret == 0:

					# Promote pending files:
					for copiedf in rp.filesin:
						try:
							dfsstat = self.dfs_map[copiedf]
						except KeyError:
							# Can't have been present at the start
							continue
						if freed_worker.wid in dfsstat["pending"]:
							print copiedf, "successfully copied to", freed_worker.address, "/", freed_worker.wid
							dfsstat["pending"].remove(freed_worker.wid)
							dfsstat["complete"].append(freed_worker.wid)

					# Verify output files:
					for madef in rp.filesout:
						check_proc = self.getrunner(rp.worker)
						check_proc.append("stat %s && touch %s" % (self.abs_dfs_file(madef), self.abs_done_file(madef)))
						try:
							subprocess.check_call(check_proc)
							print "Task produced", madef, "as expected"
							if madef not in self.dfs_map:
								self.dfs_map[madef] = {"pending": [], "complete": []}
							self.dfs_map[madef]["complete"].append(freed_worker.wid)
						except subprocess.CalledProcessError:
							print "Task promised to produce", madef, "but didn't; future tasks likely to fail"

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
					jobclass = self.classes[rp.classname]
	                                jobclass["lastwph"] = newwph
	                                print "Task completed %g units of work in %g hours; new wph = %g" % (workdone, runhours, newwph)

					if "time_reward" in jobclass and jobclass["time_reward"] is not None:
						reward_gained = actual_reward(jobclass["time_reward"], runhours)
						print "Reward gained: %f" % reward_gained
						self.total_reward += reward_gained
        
					del self.procs[pid]

				else:

					# For now just assume file copying failed:
					for failedf in rp.filesin:
						try:
							dfsstat = self.dfs_map[failedf]
						except KeyError:
							# Probably not present at the beginning; possibly removed in the interim
							continue
						if freed_worker.wid in dfsstat["pending"]:
							print "Assuming task also failed to copy", failedf
							dfsstat["pending"].remove(freed_worker.wid)

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

        def addworkitem(self, cmd, classname, maxcores, mempercore, estsize, filesin, filesout):

		mempercore = int(mempercore)
		maxcores = int(maxcores)
		estsize = float(estsize)

		def filelist(fstr):
			flist = fstr.split(":")
			return [f.strip() for f in flist if len(f.strip()) > 0]

		with self.lock:

			newpid = self.nextpid
			newproc = Task(cmd, newpid, classname, maxcores, mempercore, estsize, filelist(filesin), filelist(filesout), self)
			self.nextpid += 1
			self.procs[newpid] = newproc
			newproc.queue_time = datetime.datetime.now()
			self.pending.append(newproc)
                        print "Queue work item", newpid, cmd
			self.trystartprocs()
			return json.dumps({"pid": newpid})

	def delworkitem(self, tid):
		
		tid = int(tid)

		with self.lock:
			rp = self.procs[tid]
			if rp.proc is None:
				self.pending.remove(rp)
				del self.procs[tid]
				return {"pid": tid, "status": "deleted_queued"}
			else:
				rp.proc.terminate()
				return {"pid": tid, "status": "terminated"}

        def newworker(self, address, cores, memory):
                
                wid = self.nextwid
                self.nextwid += 1
                print "Add worker", wid, address
                return Worker(address, wid, cores, memory)

	def addworker(self, address, cores, memory):
		
		cores = int(cores)
		memory = int(memory)

		with self.lock:

			if any(w.address == address for w in self.workers.itervalues()):
				raise Exception("We already have a worker with that address")

                        newworker = self.newworker(address, cores, memory)
                        self.workers[newworker.wid] = newworker

			# Enable worker-to-worker SSH:
			copycmd = self.getcopier(newworker, [os.path.join(os.path.expanduser("~"), ".ssh/id_rsa")], "/home/%s/.ssh/scanfs_key" % self.worker_login_username)
			subprocess.check_call(copycmd)

			# Get existing DFS map, if any:
			runner = self.getrunner(newworker)
			runner.extend(["find", scanfs_root])
			find_output = subprocess.check_output(runner)
			for l in find_output.split("\n"):
				if l.startswith(scanfs_root):
					l = l[len(scanfs_root):].strip()
					if l.startswith("/"):
						l = l[1:]
					if len(l) == 0:
						continue
					path, basename = os.path.split(l)
					if basename[0] != "." or not basename.endswith(".done"):
						continue
					l = os.path.join(path, basename[1:-len(".done")])
					print "New worker", newworker.address, "seems to already have", l
					if l not in self.dfs_map:
						self.dfs_map[l] = {"complete": [], "pending": []}
					self.dfs_map[l]["complete"].append(newworker.wid)

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

	def gettotalreward(self):
		return str(self.total_reward)

	def getqueuerewardloss(self):
		return str(self.queue_reward_loss)

	def getscalerewardloss(self):
		return str(self.scale_reward_loss)

	def notifypressure(self, cut_factor):
		self.worker_pool_size_threshold *= float(cut_factor)
		self.worker_pool_size_dynamic = True

	def dfsput(self, path):

		if path.startswith("/"):
			path = path[1:]

		with self.lock:
			if path in self.dfs_map:
				raise Exception("File already in DFS")
			put_worker = random.choice([v for v in self.workers.values() if not v.delete_pending])
			self.dfs_map[path] = {"pending": [put_worker.wid], "complete": []}

		print >>sys.stderr, "Uploading", path, "to", put_worker.address, "/", put_worker.wid
			
		put_path = os.path.join(scanfs_root, path)
		put_path_dirname, put_path_basename = os.path.split(put_path)
		put_donefile = os.path.join(put_path_dirname, ".%s.done" % put_path_basename)

		put_cmd = self.getrunner(put_worker)
		put_cmd.extend(["/bin/mkdir", "-p", put_path_dirname, ";", "/bin/cat", "-", ">", put_path, ";", "touch", put_donefile])
		put_proc = subprocess.Popen(put_cmd, stdin=subprocess.PIPE)
		try:
			shutil.copyfileobj(cherrypy.request.body, put_proc.stdin)
			put_proc.stdin.close()
			ret = put_proc.wait()
			if ret != 0:
				raise Exception("Unexpected return code %d" % ret)
		except Exception as e:
			with self.lock:
				del self.dfs_map[path]
			raise e

		with self.lock:
			self.dfs_map[path] = {"complete": [put_worker.wid], "pending": []}

	def dfsget(self, path):
		
		if path.startswith("/"):
			path = path[1:]

		with self.lock:
			if path not in self.dfs_map:
				raise Exception("Not found")
			if len(self.dfs_map[path]["complete"]) == 0:
				raise Exception("No replicas")
			get_wid = random.choice(self.dfs_map[path]["complete"])

		get_worker = self.workers[get_wid]
		get_path = os.path.join(scanfs_root, path)
		
		print "Get", path, "from", get_worker.address, "/", get_worker.wid
		get_cmd = self.getrunner(get_worker)
		get_cmd.extend(["/bin/cat", get_path])
		get_proc = subprocess.Popen(get_cmd, stdout=subprocess.PIPE)

		return cherrypy.lib.static.serve_fileobj(get_proc.stdout)		

	def dfsfind(self):

		with self.lock:
			return json.dumps(self.dfs_map)

	def dfsrm(self, path):

		if path.startswith("/"):
			path = path[1:]

		with self.lock:
			if path not in self.dfs_map:
				raise Exception("Not found")
			if len(self.dfs_map[path]["pending"]) != 0:
				raise Exception("In use")
			delfrom = self.dfs_map[path]["complete"]
			del self.dfs_map[path]

		real_path = os.path.join(scanfs_root, path)
		dirname, basename = os.path.split(real_path)
		donefile_path = os.path.join(dirname, ".%s.done" % basename)

		for wid in delfrom:

			worker = self.workers[wid]
			print >>sys.stderr, "Delete", path, "from", worker.address, "/", worker.wid
			cmd = self.getrunner(worker)
			cmd.extend(["/bin/rm", real_path, ";", "/bin/rm", donefile_path])
			subprocess.check_call(cmd)

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
cherrypy.server.max_request_body_size = 1024 ** 4

cherrypy.quickstart(sched)

