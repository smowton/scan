
import params
from heapq import heappush, heappop
import random

# If nmachines is None, use dynamic horizontal scaling
# If machine_specs entries are None, use dynamic vertical scaling

class SimState:

    average_length_memory_factor = 0.9

    def __init__(self, nmachines, machine_specs, phase_splits, arrival_process, stop_time, debug, plot):

        self.now = 0
        self.event_queue = []
        self.machine_queues = [[] for x in machine_specs]
        self.machine_queue_average_lengths = [0.0 for x in machine_specs]
        self.machine_queue_average_service_intervals = [0.0 for x in machine_specs]
        self.active_machines = [[] for x in machine_specs]
        self.nmachines = nmachines
        self.machine_specs = machine_specs
        self.phase_splits = phase_splits
        self.arrival_process = arrival_process
        self.total_cost = 0
        self.total_reward = 0
        self.stop_time = stop_time
        self.debug = debug
        self.plot = plot

        for i, (splits, can_split) in enumerate(zip(self.phase_splits, params.can_split_phase)):
            if splits > 1 and not can_split:
                raise Exception("Not allowed to split phase %d" % i)

        arrival_time, first_event = arrival_process.next()
        heappush(self.event_queue, (0, first_event))
        heappush(self.event_queue, (1.0, UpdateAveragesEvent()))

    def update_average_lengths(self):

        for i, q in enumerate(self.machine_queues):
            oldval = self.machine_queue_average_lengths[i]
            memfact = SimState.average_length_memory_factor
            self.machine_queue_average_lengths[i] = (memfact * oldval) + ((1 - memfact) * float(len(q)))
            if self.debug:
                print "Queue", i, "current length", len(q), "new average", self.machine_queue_average_lengths[i]

        heappush(self.event_queue, (self.now + 1.0, UpdateAveragesEvent()))

    def active_cores(self):
        # If nmachines contains integers then our machines are always up.
        if self.nmachines[0] is not None:
            return sum([x * y for (x, y) in zip(self.nmachines, self.machine_specs)])
        else:
            return sum([sum([m.stage.active_cores for m in active_list]) for active_list in self.active_machines])

    def active_machine_count(self):

        if self.nmachines[0] is not None:
            return sum(self.nmachines)
        else:
            return sum([len(l) for l in self.active_machines])

    def run(self):

        while self.now < self.stop_time:

            (event_time, event) = heappop(self.event_queue)

            # Pay for core usage since the last event:
            elapsed = event_time - self.now
            self.total_cost += params.concurrent_cores_hired_to_cost(self.active_cores()) * elapsed

            self.now = event_time

            if self.debug:
                print self.now

            if self.plot:
                for i, active_list in enumerate(self.active_machines):
                    print "%g,%d,%d,%d" % (self.now, i, sum([x.stage.active_cores for x in active_list]), len(active_list))

            event.run(self)

    # Add a new job to the appropriate queue. Try to run it right away
    # if the queue was empty.
    def queue_job_next_stage(self, job):

        nsplits = self.phase_splits[job.current_stage] if not job.gather_started else 1
        stage = JobStage(job, nsplits)

        queueidx = stage.queue_idx(self)
        queue = self.machine_queues[queueidx]

        queue_was_empty = len(queue) == 0
        
        for i in range(nsplits):
            split = JobSplit(stage, i)
            queue.append(split)

        if queue_was_empty:
            while self.try_run_next_split(queueidx):
                pass

    # If possible, run a split for the SplitTracker at the head of the given queue
    def try_run_next_split(self, queueidx):

        queue = self.machine_queues[queueidx]
        if len(queue) == 0:
            return False

        split = queue[0]

        def should_defer():

            # Decide whether to defer a single split of a job. Thus all costs are for a single split, not the whole job.

            # Don't defer if there is no work running.
            if len(self.active_machines[queueidx]) == 0:
                return False

            # Don't defer if machines are statically allocated --
            # it'll only go to waste otherwise.
            if self.nmachines[queueidx] is not None:
                return False
            
            # If hiring now would draw from the lowest cost tier then just do it.
            active = self.active_cores()
            if split.stage.is_gather:
                cores_per_machine = 1
            else:
                cores_per_machine = self.machine_specs[queueidx]
            if cores_per_machine is None:
                cores_per_machine = split.stage.active_cores
            if cores_per_machine is None:
                cores_per_machine = 1
            hire_now_tier = params.core_tier(active + cores_per_machine)
            if hire_now_tier == 0:
                return False

            # Would we save cost by waiting to drop a cost tier instead of hiring now?
            this_split_time = split.stage.estimate_split_time(self, dynamic_cores = 1)
            cores_to_drop_tier = (active + cores_per_machine) - sum([x["cores"] for x in params.core_cost_tiers[:hire_now_tier]])
            saving = params.core_cost_tiers[hire_now_tier]["cost"] - params.core_cost_tiers[hire_now_tier - 1]["cost"]
            saving *= this_split_time

            # What would it cost to extend that time by the expected queueing time, vs. starting it now?
            # Firstly how long a delay are we proposing? This is the time until enough active machines finish:
            next_finish_splits = sorted(self.active_machines[queueidx], key = lambda x: x.split_finish_time, reverse = True)

            i = 0
            defer_delay = None
            while cores_to_drop_tier >= 0 and i <= len(next_finish_splits):
                cores_to_drop_tier -= next_finish_splits[-i].stage.active_cores
                defer_delay = next_finish_splits[-i].split_finish_time - self.now

            if cores_to_drop_tier > 0:
                splits_to_drop_tier = (cores_to_drop_tier + (cores_per_machine - 1)) / cores_per_machine
                defer_delay += (this_split_time * (float(jobs_to_drop_tier) / len(self.active_machines[queueidx])))

            def job_defer_penalty(qjob, start_delay, defer_delay):

                # How long will this job's run be in all?
                # Time already passed:
                total_duration = ((self.now - qjob.start_time) if qjob.start_time is not None else 0)

                # Time waiting to get to the front of the queue:
                total_duration += start_delay

                # Time to run remaining queue stages:
                # Since this is a cost knee point, assume we would use one core if given the choice.
                # If we are using splits then part of the job may already be running; however
                # given this piece is still in the queue it will take at least one split duration to finish.
                total_duration += qjob.estimate_finish_time(self, dynamic_cores_per_stage = 1)                
                
                reward_start_now = qjob.estimate_reward(total_duration)
                reward_start_deferred = qjob.estimate_reward(total_duration + defer_delay)
                return reward_start_now - reward_start_deferred
            
            # Assumption here: none of the jobs queued behind me will themselves skip forwards
            # by hiring another machine.

            next_start_time = self.now
            total_defer_penalty = 0
            unique_jobs = set()

            for qsplit in queue:

                qjob = qsplit.stage.job

                # There might be multiple splits of the same job queued up.
                # Don't double-count the penalty for slowing the job down (but do bump the start time)
                if qjob not in unique_jobs:
                    total_defer_penalty += job_defer_penalty(qjob, next_start_time - self.now, defer_delay)
                unique_jobs.add(qjob)

                if len(next_finish_splits) != 0:
                    next_start_time = next_finish_splits.pop().split_finish_time
                else:
                    # Estimate: on average splits further back in the queue will get to start on average
                    # every stage_time / n_active_splits seconds.
                    next_start_time += (qsplit.stage.estimate_split_time(self, dynamic_cores = 1) / len(self.active_machines[queueidx]))
                
            balance = saving - total_defer_penalty

            if self.debug:
                print "Considered deferring", str(split), "for", defer_delay, "saving", saving, "qlen", len(queue), "total defer penalty", total_defer_penalty, "balance", balance
                
            # Avoid silly FP errors when the balance is nearly zero
            return balance > 1

        def pick_dynamic_cores():

            # Select the number of cores for a *job* stage. This means that when using splits, we are choosing for *all* of them.

            if params.dynamic_core_greed_factor == 1:
                return 1

            # Gather stages are always single-cored.
            if split.stage.is_gather:
                return 1

            best_cores = 1
            best_reward = 0

            job = split.stage.job

            active = self.active_cores()
            time_already_passed = self.now - (job.start_time if job.start_time is not None else self.now)
            baseline_stage_time = job.estimate_stage_time(self, dynamic_cores = 1, include_gather_time = True)
            baseline_stage_cost = (params.concurrent_cores_hired_to_cost(active + 1) - params.concurrent_cores_hired_to_cost(active)) * baseline_stage_time
            baseline_rest_time = job.estimate_finish_time(self, dynamic_cores_per_stage = 1, from_stage = job.current_stage + 1)
            baseline_total_time = baseline_stage_time + baseline_rest_time + time_already_passed

            for cores in params.dynamic_core_choices[1:]:

                # How much reward for speeding the stage up, assuming all other stages run as baseline?
                stage_time = job.estimate_stage_time(self, dynamic_cores = cores, include_gather_time = True) 
                total_time = time_already_passed + stage_time + baseline_rest_time
                reward = job.estimate_reward(total_time) - job.estimate_reward(baseline_total_time)

                # How much cost for hiring the multi-core version?
                # Note that the gather stage is unaltered, so we consider the *split* time here.
                split_time = split.stage.estimate_split_time(self, dynamic_cores = cores)
                active_cores = self.active_cores()
                active_splits = sum([len(x) for x in self.active_machines])
                split_cost = (params.concurrent_cores_hired_to_cost(active_cores + cores) - params.concurrent_cores_hired_to_cost(active_cores)) * split_time

                # How much cost because we'll force future tasks to delay or reduce their core count?
                # The greed factor is a number indicating what multiplier on top of current utilisation we should
                # assume is just about to happen when determining how much we'll harm other tasks.
                # A value of 1 produces maximum greed. A value of 10 would assume we're about to be deluged
                # with work (rising to 10x the current load), and so picking a high core count will do a lot of harm.

                predicted_splits = (active_splits * params.dynamic_core_greed_factor) + 1
                new_splits = predicted_splits - active_splits
                new_cores = new_splits * cores
                new_cores_cost = (params.concurrent_cores_hired_to_cost(active_cores + new_cores) - params.concurrent_cores_hired_to_cost(active_cores)) * split_time
                average_cost_per_split = float(new_cores_cost) / new_splits

                if self.debug:
                    print "Running with", cores, "cores would yield reward", reward, "and average cost", average_cost_per_split, "total", reward - average_cost_per_split

                reward -= average_cost_per_split

                if reward > best_reward:
                    best_reward = reward
                    best_cores = cores
                    
            if self.debug:
                print "Running with", best_cores, "cores"

            return best_cores

        run_now = True
        could_run_now = True

        if self.nmachines[queueidx] is not None and len(self.active_machines[queueidx]) == self.nmachines[queueidx]:
            # No machines available
            run_now = False
            could_run_now = False
        elif should_defer():
            run_now = False

        if not run_now:
            if self.debug:
                if could_run_now:
                    print "Will defer", str(split), "queued for machine type", queueidx
                else:
                    print "Can't run", str(split), "right away, queued for machine type", queueidx
            return False

        # Do it now. Remove from queue and determine the next event for this job.

        queue.pop(0)

        if split.stage.is_gather:
            cores = 1
        else:
            cores = self.machine_specs[queueidx]
        # Using dynamic vertical scaling?
        if cores is None:
            cores = split.stage.active_cores
        if cores is None:
            cores = pick_dynamic_cores()

        vm_startup_delay = params.vm_startup_delay if self.nmachines[queueidx] is None else 0

        self.active_machines[queueidx].append(split)
        split.schedule_split(self, cores, vm_startup_delay)

        return True

    def release_split_machine(self, split):

        queue_idx = split.stage.queue_idx(self)
        machines = self.active_machines[queue_idx]
        machines.remove(split)

        self.try_run_next_split(queue_idx)

    def queue_next_arrival(self):
        
        arrival_time, event = self.arrival_process.next()
        heappush(self.event_queue, (self.now + arrival_time, event))

class ArrivalEvent:

    def __init__(self, jobs):
        self.jobs = jobs

    def run(self, state):
        for job in self.jobs:
            state.queue_job_next_stage(job)
        state.queue_next_arrival()

class SplitDoneEvent:

    def __init__(self, split):
        self.split = split

    def run(self, state):

        state.release_split_machine(self.split)
        self.split.split_done(state)

next_job_id = 0
def fresh_job_id():
    global next_job_id
    ret = next_job_id
    next_job_id += 1
    return ret

# JobSplit keeps track of state that only applies to one split
class JobSplit:

    def __init__(self, stage, split_idx):

        self.stage = stage
        self.split_start_time = None
        self.split_finish_time = None
        self.split_idx = split_idx

    def __str__(self):
        
        ret = str(self.stage.job)
        if self.stage.total_splits != 1:
            ret += " (split %d)" % (self.split_idx + 1)
        elif self.stage.is_gather:
            ret += " (gather phase)"
        return ret

    def schedule_split(self, state, cores, vm_startup_delay):

        self.stage.schedule_split(state, cores)

        self.split_start_time = state.now
        predicted_runtime = self.stage.estimate_split_time(state, dynamic_cores = cores) + vm_startup_delay
        self.split_finish_time = state.now + predicted_runtime

        if state.debug:
            print "Start", str(self), "stage", self.stage.job.current_stage, "at", state.now, "expected finish", self.split_finish_time
            
        real_runtime = params.predicted_to_real_time(predicted_runtime)
        split_end_event = SplitDoneEvent(self)
        heappush(state.event_queue, (state.now + real_runtime, split_end_event))
        
    def split_done(self, state):

        self.stage.split_done(state)

# JobStages keep track of per-stage state.
# Job keeps track of state that spans across stages.
class JobStage:

    def __init__(self, job, splits):

        self.job = job
        self.total_splits = splits
        self.splits_scheduled = 0
        self.splits_done = 0
        self.active_cores = None
        self.is_gather = self.job.gather_started
        self.stage_idx = self.job.current_stage

    def schedule_split(self, state, cores):

        if self.active_cores is not None and self.active_cores != cores:
            raise Exception("Should use the same number of cores per split")
        self.active_cores = cores
        self.splits_scheduled += 1

        self.job.schedule_split(state)

    def split_done(self, state):
        
        self.splits_done += 1
        if self.splits_done < self.total_splits:
            if state.debug:
                print "%s: Only %d/%d splits completed" % (str(self.job), self.splits_done, self.total_splits)
            return

        if self.total_splits > 1:
            if state.debug:
                print "%s: All splits completed; scheduling gather" % str(self.job)
            self.job.gather_started = True
            state.queue_job_next_stage(self.job)
        else:
            self.gather_done(state)

    def gather_done(self, state):

        if state.debug and self.is_gather:
            print "%s: Gather done" % str(self.job)

        self.job.current_stage += 1
        self.job.gather_started = False

        if self.job.done():
            self.job.credit_reward(state)
        else:
            state.queue_job_next_stage(self.job)

    def estimate_split_time(self, state, dynamic_cores):

        if self.active_cores is not None:
            dynamic_cores = self.active_cores

        if self.is_gather and dynamic_cores != 1:
            raise Exception("Gather phases must run single-cored")

        if self.is_gather:
            return self.job.gather_time()
        else:
            return self.job.estimate_stage_time(state, dynamic_cores, include_gather_time = False)

    def queue_idx(self, state):

        if len(state.machine_queues) == 1:
            return 0
        elif self.is_gather:
            return len(state.machine_queues) - 1
        else:
            return self.stage_idx

class Job:

    def __init__(self, nrecords):
        self.nrecords = nrecords
        self.current_stage = 0
        self.gather_started = False
        self.start_time = None
        self.job_id = fresh_job_id()

    def __str__(self):
        return "Job %d (size %g)" % (self.job_id, self.nrecords)

    def done(self):
        return self.current_stage == 7

    def schedule_split(self, state):
        if self.start_time is None:
            self.start_time = state.now

    def gather_time(self):
        return params.gather_time(self.nrecords, self.current_stage)
        
    def estimate_finish_time(self, state, dynamic_cores_per_stage, from_stage = -1, to_stage = 7, include_gather_time = True):

        # Some part of this job is still in a queue waiting to run. Estimate time to finish the rest of the job.
        def cores_for_stage(i):
            machine_type = i if len(state.machine_specs) > 1 else 0
            ret = state.machine_specs[machine_type]
            # Dynamic scaling in use?
            if ret is None:
                ret = dynamic_cores_per_stage
            return ret
        if from_stage == -1:
            from_stage = self.current_stage

        if from_stage == self.current_stage and self.gather_started:
            from_stage += 1
            extra_gather = params.gather_time(self.nrecords, self.current_stage)
        else:
            extra_gather = 0

        return extra_gather + sum([params.processing_time(self.nrecords, cores_for_stage(i), state.phase_splits[i], i, include_gather_time) for i in range(from_stage, to_stage)])

    def estimate_stage_time(self, state, dynamic_cores, include_gather_time):
        return self.estimate_finish_time(state, dynamic_cores, from_stage = self.current_stage, to_stage = self.current_stage + 1, include_gather_time = include_gather_time)

    def estimate_reward(self, running_time):
        return params.reward(running_time, self.nrecords)

    def credit_reward(self, state):
        reward = params.reward(state.now - self.start_time, self.nrecords)
        if state.debug:
            print "Job", str(self), "done; credit", reward
        state.total_reward += reward

class ArrivalProcess:

    def __init__(self, mean_arrival, mean_jobs, jobs_var, mean_records, records_var):
        self.mean_arrival = mean_arrival
        self.arrival_lambda = 1.0 / mean_arrival
        self.mean_jobs = mean_jobs
        self.jobs_var = jobs_var
        self.mean_records = mean_records
        self.records_var = records_var

    def next(self):
        
        arrival_interval = random.expovariate(self.arrival_lambda)
        if arrival_interval == float("inf") or arrival_interval <= 0:
            arrival_interval = self.mean_arrival

        njobs = int(random.normalvariate(self.mean_jobs, self.jobs_var))
        if njobs <= 0:
            njobs = 1
        
        jobs = []
        for i in range(njobs):
            jobs.append(Job(random.normalvariate(self.mean_records, self.records_var)))

        return (arrival_interval, ArrivalEvent(jobs))

class UpdateAveragesEvent:

    def run(self, state):
        state.update_average_lengths()

