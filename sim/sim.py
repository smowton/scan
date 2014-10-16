
import params
from heapq import heappush, heappop
import random

# If nmachines is None, use dynamic horizontal scaling
# If machine_specs entries are None, use dynamic vertical scaling

class SimState:

    def __init__(self, nmachines, machine_specs, arrival_process, stop_time, debug):

        self.now = 0
        self.event_queue = []
        self.machine_queues = [[] for x in machine_specs]
        self.active_machines = [[] for x in machine_specs]
        self.nmachines = nmachines
        self.machine_specs = machine_specs
        self.arrival_process = arrival_process
        self.total_cost = 0
        self.total_reward = 0
        self.stop_time = stop_time
        self.debug = debug

        arrival_time, first_event = arrival_process.next()
        heappush(self.event_queue, (0, first_event))

    def active_cores(self):
        # If nmachines contains integers then our machines are always up.
        if self.nmachines[0] is not None:
            return sum([x * y for (x, y) in zip(self.nmachines, self.machine_specs)])
        else:
            return sum([sum([m.active_cores for m in active_list]) for active_list in self.active_machines])

    def run(self):

        while self.now < self.stop_time:

            (event_time, event) = heappop(self.event_queue)

            # Pay for core usage since the last event:
            elapsed = event_time - self.now
            self.total_cost += params.concurrent_cores_hired_to_cost(self.active_cores()) * elapsed

            self.now = event_time

            if self.debug:
                print self.now

            for i, active_list in enumerate(self.active_machines):
                print "%g,%d,%d" % (self.now, i, len(active_list))

            event.run(self)

    def try_run_job(self, job, at_head = False):

        if job.done():
            job.credit_reward(self)
            return

        queueidx = job.next_stage if len(self.machine_queues) > 1 else 0
        queue = self.machine_queues[queueidx]

        run_now = True
        could_run_now = True

        def should_defer():

            # Don't defer if the queue is empty.
            if len(self.active_machines[queueidx]) == 0:
                return False

            # Don't defer if machines are statically allocated --
            # it'll only go to waste otherwise.
            if self.nmachines[queueidx] is not None:
                return False
            
            # Would we save cost by waiting for an existing machine instead of hiring another?
            active = self.active_cores()
            cores_per_machine = self.machine_specs[queueidx]
            if cores_per_machine is None:
                cores_per_machine = 1
            costs = map(params.concurrent_cores_hired_to_cost, range(active - cores_per_machine, active + (2 * cores_per_machine), cores_per_machine))
            saving = (costs[2] - costs[1]) - (costs[1] - costs[0])
            if saving <= 0:
                return False
            
            # How much would we save, considering when the next job will finish?
            defer_until = min([x.stage_finish_time - self.now for x in self.active_machines[queueidx]])
            saving *= (defer_until - self.now)

            # Roughly how much time to go for this job? Since we're at a cost knee point,
            # assume we would favour running it with one core throughout the remainder of the run
            # if we have a choice.
            time = job.estimate_finish_time(self, dynamic_cores_per_stage = 1)

            # What would it cost to extend that time by defer_until - now?
            reward_start_now = job.estimate_reward(time)
            reward_start_deferred = job.estimate_reward(time + (defer_until - self.now))
            
            balance = saving - (reward_start_now - reward_start_deferred)
            if self.debug:
                print "Considered deferring", str(job), "until", defer_until, "saving", saving, "rewards", reward_start_now, reward_start_deferred, "balance", balance
                
            # Avoid silly FP errors when the balance is nearly zero
            return balance > 1

        def pick_dynamic_cores():

            if greed_factor == 1:
                return 1

            best_cores = 1
            best_reward = 0

            time_already_passed = self.now - (job.start_time if job.start_time is not None else self.now)
            baseline_stage_time = job.estimate_stage_time(self, dynamic_cores = 1)
            baseline_stage_cost = (params.concurrent_cores_hired_to_cost(active_cores + 1) - params.concurrent_cores_hired_to_cost(active_cores)) * baseline_stage_time
            baseline_rest_time = job.estimate_finish_time(self, dynamic_cores_per_stage = 1, from_stage = job.next_stage + 1)
            baseline_total_time = baseline_stage_time + baseline_rest_time + time_already_passed

            for cores in params.dynamic_core_choices[1:]:

                # How much reward for speeding the stage up, assuming all other stages run as baseline?
                stage_time = job.estimate_stage_time(self, dynamic_cores = cores) 
                total_time = time_already_passed + stage_time + baseline_rest_time
                reward = job.estimate_reward(total_time) - job.estimate_reward(baseline_total_time)

                # How much cost for hiring the multi-core version?
                active_cores = self.active_cores()
                active_jobs = sum([len(x) for x in self.active_machines])
                stage_cost = (params.concurrent_cores_hired_to_cost(active_cores + cores) - params.concurrent_cores_hired_to_cost(active_cores)) * stage_time

                # How much cost because we'll force future tasks to delay or reduce their core count?
                # The greed factor is a number indicating what multiplier on top of current utilisation we should
                # assume is just about to happen when determining how much we'll harm other tasks.
                # A value of 1 produces maximum greed. A value of 10 would assume we're about to be deluged
                # with work (rising to 10x the current load), and so picking a high core count will do a lot of harm.

                predicted_jobs = (active_jobs * params.dynamic_core_greed_factor) + 1
                new_jobs = predicted_jobs - active_jobs
                new_cores = new_jobs * cores
                new_cores_cost = (params.concurrent_cores_hired_to_cost(active_cores + new_cores) - params.concurrent_cores_hired_to_cost(active_cores)) * stage_time
                average_cost_per_job = new_cores_cost / new_jobs
                
                reward -= average_cost_per_job

                if reward > best_reward:
                    best_reward = reward
                    best_cores = cores

            return best_cores

        if len(queue) > 0 and not at_head:
            run_now = False
            could_run_now = False
        elif self.nmachines[queueidx] is not None and len(self.active_machines[queueidx]) == self.nmachines[queueidx]:
            # No machines available
            run_now = False
            could_run_now = False
        elif should_defer():
            run_now = False

        if not run_now:
            if self.debug:
                if could_run_now:
                    print "Deferring", str(job), "queued for machine type", queueidx
                else:
                    print "Can't run", str(job), "right away, queued for machine type", queueidx
            queue.append(job)
            return False

        # Do it now.

        cores = self.machine_specs[queueidx]
        # Using dynamic vertical scaling?
        if cores is None:
            cores = pick_dynamic_cores()

        vm_startup_delay = params.vm_startup_delay if self.nmachines[queueidx] is None else 0

        self.active_machines[queueidx].append(job)
        job.schedule_next_stage(self, cores, vm_startup_delay)

        return True

    def release_job_machine(self, job):

        queueidx = (job.next_stage - 1) if len(self.machine_queues) > 1 else 0
        machines = self.active_machines[queueidx]
        machines.remove(job)

        queue = self.machine_queues[queueidx]
        if len(queue) > 0:
            ran = self.try_run_job(queue[0], at_head = True)
            if ran:
                queue.pop(0)

    def queue_next_arrival(self):
        
        arrival_time, event = self.arrival_process.next()
        heappush(self.event_queue, (self.now + arrival_time, event))

class ArrivalEvent:

    def __init__(self, jobs):
        self.jobs = jobs

    def run(self, state):
        for job in self.jobs:
            state.try_run_job(job)
        state.queue_next_arrival()

class JobDoneEvent:

    def __init__(self, job):
        self.job = job

    def run(self, state):
        state.release_job_machine(self.job)
        state.try_run_job(self.job)

next_job_id = 0
def fresh_job_id():
    global next_job_id
    ret = next_job_id
    next_job_id += 1
    return ret

class Job:

    def __init__(self, nrecords):
        self.nrecords = nrecords
        self.next_stage = 0
        self.start_time = None
        self.stage_start_time = None
        self.stage_finish_time = None
        self.job_id = fresh_job_id()
        self.active_cores = None

    def __str__(self):
        return "Job %d (size %d)" % (self.job_id, self.nrecords)

    def done(self):
        return self.next_stage == 7

    def schedule_next_stage(self, state, cores, vm_startup_delay):

        self.active_cores = cores
        self.stage_start_time = state.now
        self.stage_finish_time = state.now + params.processing_time(self.nrecords, cores, 1, self.next_stage) + vm_startup_delay
        if self.start_time is None:
            self.start_time = state.now

        if state.debug:
            print "Start job", str(self), "stage", self.next_stage, "at", state.now, "expected finish", self.stage_finish_time

        stage_end_event = JobDoneEvent(self)
        heappush(state.event_queue, (self.stage_finish_time, stage_end_event))
        self.next_stage += 1

    def estimate_finish_time(self, state, dynamic_cores_per_stage, from_stage = -1, to_stage = 7):
        # Assume we are not currently running; estimate finish of whole job
        def cores_for_stage(i):
            machine_type = i if len(state.machine_specs) > 1 else 0
            ret = state.machine_specs[machine_type]
            # Dynamic scaling in use?
            if ret is None:
                ret = dynamic_cores_per_stage
            return ret
        if from_stage == -1:
            from_stage = self.next_stage
        return sum([params.processing_time(self.nrecords, cores_for_stage(i), 1, i) for i in range(from_stage, to_stage)])

    def estimate_stage_time(self, state, dynamic_cores):
        return self.estimate_finish_time(state, dynamic_cores, from_stage = self.next_stage, to_stage = self.next_stage + 1)

    def estimate_reward(self, running_time):
        return params.reward(self.nrecords, running_time)

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
        if arrival_interval == float("inf"):
            arrival_interval = self.mean_arrival

        njobs = int(random.normalvariate(self.mean_jobs, self.jobs_var))
        
        jobs = []
        for i in range(njobs):
            jobs.append(Job(random.normalvariate(self.mean_records, self.records_var)))

        return (arrival_interval, ArrivalEvent(jobs))


