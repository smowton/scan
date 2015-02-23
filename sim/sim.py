
import params
from heapq import heappush, heappop
import random
import itertools
import numbers

# If nmachines is None, use dynamic horizontal scaling
# If machine_specs entries are None, use dynamic vertical scaling

class SimState:

    average_length_memory_factor = 0.9

    def __init__(self, nmachines, ncores, machine_specs, phase_splits, arrival_process, stop_time, debug, plot, hscale_algorithm, hscale_params, vscale_algorithm, vscale_params):

        self.now = 0
        self.event_queue = []
        if ncores is not None:
            self.machine_queues = [[]]
        else:
            self.machine_queues = [[] for x in machine_specs]
        self.machine_queue_average_lengths = [0.0 for x in machine_specs]
        self.machine_queue_average_service_intervals = [0.0 for x in machine_specs]
        self.active_splits_by_type = [[[] for tier in params.core_cost_tiers] for x in machine_specs]
        self.nmachines = nmachines
        self.ncores = ncores
        self.running_cores = 0
        self.machine_specs = machine_specs
        self.phase_splits = phase_splits
        self.arrival_process = arrival_process
        self.total_cost = 0
        self.cost_by_tier = [0] * len(params.core_cost_tiers)
        self.total_reward = 0
        self.stop_time = stop_time
        self.debug = debug
        self.plot = plot
        self.hscale_algorithm = hscale_algorithm
        self.hscale_params = hscale_params
        self.vscale_params = vscale_params
        self.vscale_algorithm = vscale_algorithm
        self.completed_jobs = []

        for i, (splits, can_split) in enumerate(zip(self.phase_splits, params.can_split_phase)):
            if splits > 1 and not can_split:
                raise Exception("Not allowed to split phase %d" % i)

        arrival_time, first_event = arrival_process.next(self)
        heappush(self.event_queue, (0, first_event))
        heappush(self.event_queue, (1.0, UpdateAveragesEvent()))

        self.init_vscale_algorithm()

    def init_vscale_algorithm(self):

        if "greed" in self.vscale_params:
            self.vscale_params["greed"] = float(self.vscale_params["greed"])
        if "queuefactor" in self.vscale_params:
            self.vscale_params["queuefactor"] = float(self.vscale_params["queuefactor"])
        if "plan" in self.vscale_params:
            self.vscale_params["plan"] = [int(bit) for bit in self.vscale_params["plan"].split(":")]
        
        if self.vscale_algorithm == "longterm" or self.vscale_algorithm == "ltadaptive":

            # Find the "best" plans for executing an averagely-sized job.
            # Select 10 representitives that will actually be considered for each job
            # as its first stage reaches the front of the queue.

            recs = self.arrival_process.mean_records

            def config_stage_times(config):
                return [params.processing_time(recs, config[i], 1, i, False) for i in range(7)]

            def stage_core_time_units(config):
                stage_times = config_stage_times(config)
                return [stage_time * cores for (stage_time, cores) in zip(stage_times, config)]

            def predict_config_reward_ratio(config):
                stage_times = config_stage_times(config)
                core_time_units = stage_core_time_units(config)
                total_cost = params.core_cost_tiers[0]["cost"] * sum(core_time_units)
                total_reward = params.reward(sum(stage_times) * self.vscale_params["queuefactor"], self.arrival_process.mean_records)
                return float(total_reward) / total_cost

            def config_average_cores(config):
                total_time = sum(config_stage_times(config))
                total_core_time_units = sum(stage_core_time_units(config))
                return float(total_core_time_units / total_time)

            all_config_ratios = [(config, predict_config_reward_ratio(config)) for config in itertools.product(params.dynamic_core_choices, repeat = 7)]
            all_config_ratios = sorted(all_config_ratios, key = lambda (config, reward) : reward)

            # Never consider any options that are less rewarding than running everything single-threaded
            min_ratio = predict_config_reward_ratio([1,1,1,1,1,1,1])
            all_config_ratios = [(config, ratio) for (config, ratio) in all_config_ratios if ratio >= min_ratio]

            def make_monotonic(config_ratios):

                # Discard any candidate configuration that uses more core-hours whilst delivering lesser rewards
                kept_config_ratios = []
                for i in range(1, len(config_ratios) + 1):
                    (config, ratio) = config_ratios[-i]
                    if len(kept_config_ratios) == 0:
                        kept_config_ratios.append((config, ratio))
                    elif sum(stage_core_time_units(config)) < sum(stage_core_time_units(kept_config_ratios[-1][0])):
                        kept_config_ratios.append((config, ratio))

                kept_config_ratios.reverse()
                return kept_config_ratios

            if self.vscale_algorithm == "longterm":

                all_config_ratios = make_monotonic(all_config_ratios)

                self.candidate_configs = []
                candidate_ratios = []

                if len(all_config_ratios) > 10:
                    for i in range(10):
                        idx = int(round((float(i) / 9) * (len(all_config_ratios) - 1)))
                        config = all_config_ratios[idx][0]
                        self.candidate_configs.append((config, config_average_cores(config)))
                        candidate_ratios.append(all_config_ratios[idx][1])
                else:
                    self.candidate_configs = [(config, config_average_cores(config)) for (config, ratio) in all_config_ratios]
                    candidate_ratios = [ratio for (config, ratio) in all_config_ratios]

                if self.debug:
                    print "Using longterm configs (best ratio %g, min ratio %g):" % (all_config_ratios[-1][1], min_ratio)
                    for ((config, average_cores), ratio) in zip(self.candidate_configs, candidate_ratios):
                        print config, "avg", average_cores, "ratio", ratio

            else:
                
                # For adaptivelt operation we try to use the best possible execution plan,
                # then fall back if current workload forces us to retreat from our full planned resource usage.

                def isprefix(pref, full):
                    return full[:len(pref)] == pref

                self.ltadaptive_plans = dict()
                
                def populate_fallbacks_from(past_phases, parent_configs):

                    pp = tuple(past_phases)
                    possible_configs = [c for c in parent_configs if isprefix(pp, c)]
                    self.ltadaptive_plans[pp] = possible_configs[-1]
                    if self.debug:
                        print " " * len(past_phases), "With prefix", past_phases, "use plan", possible_configs[-1]
                    this_phase_cores = possible_configs[-1][len(past_phases)]
                    
                    for fallback_cores in params.dynamic_core_choices:
                        if fallback_cores > this_phase_cores:
                            # Fallback never increases the number of cores in a plan
                            continue
                        present_phases = past_phases + [fallback_cores]
                        if len(present_phases) < 7:
                            if self.debug:
                                if fallback_cores == this_phase_cores:
                                    print " " * len(past_phases), "Ideally use plan:"
                                else:
                                    print " " * len(past_phases), "If forced to fall back to", fallback_cores, "cores, use:"
                            populate_fallbacks_from(present_phases, possible_configs)

                populate_fallbacks_from([], [c for (c, r) in all_config_ratios])

                if self.debug:
                    for (k, v) in self.ltadaptive_plans.iteritems():
                        print k, "->", v

    def update_average_lengths(self):

        for i, q in enumerate(self.machine_queues):
            oldval = self.machine_queue_average_lengths[i]
            memfact = SimState.average_length_memory_factor
            self.machine_queue_average_lengths[i] = (memfact * oldval) + ((1 - memfact) * float(len(q)))
            if self.debug:
                print "Queue", i, "current length", len(q), "new average", self.machine_queue_average_lengths[i]

        heappush(self.event_queue, (self.now + 1.0, UpdateAveragesEvent()))

    def active_cores_of_type(self, typelist):
        return sum([sum([m.stage.active_cores for m in tier_list]) for tier_list in typelist])

    def active_cores_by_tier(self, i):
        return sum([sum([s.stage.active_cores for s in typelist[i]]) for typelist in self.active_splits_by_type])

    def active_cores_with_jobs(self):
        sum([self.active_cores_of_type(typelist) for typelist in self.active_splits_by_type])

    def active_cores(self):
        # If nmachines contains integers then our machines are always up.
        # If ncores is set then the core pool is fluid, but also always up.
        if self.nmachines[0] is not None:
            return sum([x * y for (x, y) in zip(self.nmachines, self.machine_specs)])
        elif self.ncores is not None:
            return self.ncores
        else:
            return self.active_cores_with_jobs()

    def run(self):

        while self.now < self.stop_time:

            (event_time, event) = heappop(self.event_queue)

            # Pay for core usage since the last event:
            elapsed = event_time - self.now
            for machine_type_list in self.active_splits_by_type:
                for (i, (tier, tier_list)) in enumerate(zip(params.core_cost_tiers, machine_type_list)):
                    for split in tier_list:
                        stage = split.stage
                        job = stage.job
                        new_cost = tier["cost"] * elapsed * stage.active_cores
                        job.cost += new_cost
                        self.total_cost += new_cost
                        self.cost_by_tier[i] += new_cost

            self.now = event_time

            if self.debug:
                print self.now

            if self.plot:
                for i, machine_type_list in enumerate(self.active_splits_by_type):
                    active_splits = sum([len(tier_list) for tier_list in machine_type_list])
                    active_cores = self.active_cores_of_type(machine_type_list)
                    print "%g,%d,%d,%d" % (self.now, i, active_cores, active_splits)

            event.run(self)

    # Add a new job to the appropriate queue. Try to run it right away
    # if the queue was empty.
    def queue_job_next_stage(self, job):

        nsplits = self.phase_splits[job.current_stage] if not job.gather_started else 1
        stage = JobStage(job, nsplits)

        queueidx = stage.queue_idx(self)
        queue = self.machine_queues[queueidx]

        for i in range(nsplits):
            split = JobSplit(stage, i, self.now)
            queue.append(split)

        while self.try_run_next_split(queueidx):
            pass

    # If possible, run a split for the SplitTracker at the head of the given queue
    def try_run_next_split(self, queueidx):

        queue = self.machine_queues[queueidx]
        if len(queue) == 0:
            return False

        split = queue[0]

        def should_defer(this_stage_cores):

            # Decide whether to defer a single split of a job. Thus all costs are for a single split, not the whole job.

            # Don't defer if there is no work running.
            def any_work_running():
                for typelist in self.active_splits_by_type:
                    for tierlist in typelist:
                        if len(tierlist) > 0:
                            return True
                return False

            if not any_work_running():
                return False

            # Don't defer if machines are statically allocated --
            # it'll only go to waste otherwise.
            if self.nmachines[queueidx] is not None:
                return False
            if self.ncores is not None:
                return False

            # If hiring now would draw from the lowest cost tier then just do it.
            if split.stage.is_gather:
                cores_wanted = 1
            else:
                cores_wanted = self.machine_specs[queueidx]
            if cores_wanted is None:
                cores_wanted = split.stage.active_cores
            if cores_wanted is None:
                cores_wanted = this_stage_cores

            private_tier_active_cores = self.active_cores_by_tier(0)
            cores_needed = (private_tier_active_cores + cores_wanted) - params.core_cost_tiers[0]["cores"]
            if cores_needed <= 0:
                return False

            if self.hscale_algorithm == "always":
                return False
            elif self.hscale_algorithm == "never":
                return True

            # Would we save cost by waiting for private tier cores to become free?
            this_split_time = split.stage.estimate_split_time(self, dynamic_cores = this_stage_cores)
            saving = params.core_cost_tiers[1]["cost"] - params.core_cost_tiers[0]["cost"]
            saving *= this_split_time

            # Calculate new predicted finish times for each task that is currently running, based
            # on the prediction originally entered when they started and the current time.
            def predict_finish_time(predsplit):
                if self.hscale_algorithm == "predict":
                    return max(predsplit.split_finish_time, self.now)
                elif self.hscale_algorithm == "preduniform":
                    errorfact = float(self.hscale_params["maxerror"])
                    expected_runtime = predsplit.split_finish_time - predsplit.split_start_time
                    min_runtime = (1 - errorfact) * expected_runtime
                    max_runtime = (1 + errorfact) * expected_runtime
                    min_finish_time = predsplit.split_start_time + min_runtime
                    max_finish_time = predsplit.split_start_time + max_runtime
                    if self.now <= min_finish_time:
                        # Continue to expect mean:
                        return predsplit.split_finish_time
                    elif self.now >= max_finish_time:
                        # Surely will finish any minute now...
                        return self.now
                    else:
                        # Average of remaining uniform distribution:
                        return (max_finish_time + self.now) / 2
                else:
                    raise Exception("Bad hscale algorithm: " + self.hscale_algorithm)

            # Only consider splits running at tier 0.
            # TOFIX: For the case with multiple queues, consider splits in other queues?
            split_finish_times = [(s, predict_finish_time(s)) for s in self.active_splits_by_type[queueidx][0]]
            next_finish_splits = sorted(split_finish_times, key = lambda x: x[1], reverse = True)

            # Keep the ordered splits for next time, since Python 'sorted' benefits from mostly-ordered data
            self.active_splits_by_type[queueidx][0] = [x[0] for x in next_finish_splits]

            # What would it cost to extend each queued task's start time by the expected queueing time, vs. starting it now?
            # Firstly how long a delay are we proposing? This is the time until enough active machines finish:
           
            if self.debug:
                print "Next finish splits at %g:" % self.now
                for (s, pred_time) in next_finish_splits:
                    print "Orginal prediction %g, now %g" % (s.split_finish_time, pred_time)

            i = 1
            defer_delay = None
            while cores_needed >= 0 and i <= len(next_finish_splits):
                cores_needed -= next_finish_splits[-i][0].stage.active_cores
                defer_delay = next_finish_splits[-i][1] - self.now
                i += 1

            if cores_needed > 0:
                raise Exception("Still short of private-tier cores after accounting for all running splits? Tier too small, or max cores per job too large.")

            # Avoid FP error:
            if defer_delay < 0.001:
                return True

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

                qjob_plan = guess_job_plan(qjob)
                total_duration += qjob.estimate_finish_time(self, dynamic_cores_per_stage = qjob_plan)
                
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

                # Dubiousness here: we're assuming roughly one split in, one split out -- but we could have e.g.
                # a 4-core split finishing permitting a bunch of 1-core queued jobs to proceed, or vice versa.
                # We can't do this exactly right since the queued jobs will only pick their true core count
                # when they are actually started.
                if len(next_finish_splits) != 0:
                    next_start_time = max(next_finish_splits.pop()[1], self.now)
                else:
                    # Estimate: on average splits further back in the queue will get to start on average
                    # every stage_time / n_active_splits seconds.
                    next_start_time += (qsplit.stage.estimate_split_time(self, dynamic_cores = guess_job_plan(qjob)) / len(self.active_splits_by_type[queueidx][0]))
                
            balance = saving - total_defer_penalty

            if self.debug:
                print "Considered deferring", str(split), "for", defer_delay, "saving", saving, "qlen", len(queue), "total defer penalty", total_defer_penalty, "balance", balance
                
            # Avoid silly FP errors when the balance is nearly zero
            return balance > 1

        def pick_dynamic_cores():

            # Select the number of cores for a *job* stage. This means that when using splits, we are choosing for *all* of them.

            # Gather stages are always single-cored.
            if split.stage.is_gather:
                return 1

            if self.vscale_algorithm == "greedy":
                return pick_dynamic_cores_greedy()
            elif self.vscale_algorithm == "longterm":
                return pick_dynamic_cores_longterm()
            elif self.vscale_algorithm == "ltadaptive":
                return pick_dynamic_cores_ltadaptive()
            elif self.vscale_algorithm == "constant":
                return self.vscale_params["plan"][split.stage.job.current_stage]
            else:
                raise Exception("Bad vscale algorithm " + self.vscale_algorithm)

        def pick_dynamic_cores_greedy():
            return pick_dynamic_cores_greedy_core(params.dynamic_core_choices)

        def pick_dynamic_cores_greedy_core(choices, future_dynamic_cores_fn = None):

            best_cores = choices[0]
            best_reward = 0

            job = split.stage.job

            active_cores = self.active_cores()
            active_splits = sum([sum([len(x) for x in typelist]) for typelist in self.active_splits_by_type])
            predicted_splits = (active_splits * self.vscale_params["greed"]) + 1
            new_splits = predicted_splits - active_splits

            time_already_passed = self.now - (job.start_time if job.start_time is not None else self.now)
            baseline_stage_time = job.estimate_stage_time(self, dynamic_cores = 1, include_gather_time = True)
            private_tier = params.core_cost_tiers[0]
            public_tier = params.core_cost_tiers[1]
            private_tier_used_cores = self.active_cores_by_tier(0)
            private_tier_free_cores = private_tier["cores"] - private_tier_used_cores
            baseline_cost = private_tier["cost"] if private_tier_used_cores == private_tier["cores"] else public_tier["cost"]
            baseline_cost *= baseline_stage_time
        
            if future_dynamic_cores_fn is None:
                baseline_cores = 1
            else:
                baseline_cores = future_dynamic_cores_fn(1)

            baseline_rest_time = job.estimate_finish_time(self, dynamic_cores_per_stage = baseline_cores, from_stage = job.current_stage + 1)
            baseline_total_time = baseline_stage_time + baseline_rest_time + time_already_passed
            baseline_reward = job.estimate_reward(baseline_total_time)

            for cores in choices[1:]:

                # How much reward for speeding the stage up, assuming all other stages run as indicated in the plan?
                # Note the queue delay factor is considered here, but not under the cost accounting, since this is idle time.
                stage_time = job.estimate_stage_time(self, dynamic_cores = cores, include_gather_time = True) 
                if future_dynamic_cores_fn is None:
                    rest_time = baseline_rest_time
                else:
                    rest_time = job.estimate_finish_time(self, dynamic_cores_per_stage = future_dynamic_cores_fn(cores), from_stage = job.current_stage + 1)
                total_time = time_already_passed + ((stage_time + rest_time) * self.vscale_params["queuefactor"])
                reward = job.estimate_reward(total_time) - baseline_reward

                # How much cost for hiring the multi-core version?
                # Note that the gather stage is unaltered, so we consider the *split* time here.
                split_time = split.stage.estimate_split_time(self, dynamic_cores = cores)

                # How much cost because we'll force future tasks to delay or reduce their core count?
                # The greed factor is a number indicating what multiplier on top of current utilisation we should
                # assume is just about to happen when determining how much we'll harm other tasks.
                # A value of 1 produces maximum greed. A value of 10 would assume we're about to be deluged
                # with work (rising to 10x the current load), and so picking a high core count will do a lot of harm.

                new_cores = new_splits * cores
                new_private_cores = min(new_cores, private_tier_free_cores)
                new_public_cores = new_cores - new_private_cores

                new_cores_cost = (new_private_cores * private_tier["cost"]) + (new_public_cores * public_tier["cost"])
                new_cores_cost *= split_time
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

        def pick_dynamic_cores_longterm():

            job = split.stage.job
            
            # Check for stage > 0 to allow this function to be used for tentative guesses
            # as well as the final decision of what to run.
            if job.longterm_plan is not None and job.current_stage > 0:
                return job.longterm_plan[job.current_stage]

            best_plan = None
            best_reward = 0
            active_cores = self.active_cores()
            active_splits = sum([sum([len(x) for x in typelist]) for typelist in self.active_splits_by_type])
            predicted_splits = (active_splits * self.vscale_params["greed"]) + 1
            new_splits = predicted_splits - active_splits

            private_tier = params.core_cost_tiers[0]
            public_tier = params.core_cost_tiers[1]
            private_tier_used_cores = self.active_cores_by_tier(0)
            private_tier_free_cores = private_tier["cores"] - private_tier_used_cores

            for (config, avgcores) in self.candidate_configs:

                # Calculate estimated reward for running the *entire* job to plan.
                est_time = job.estimate_finish_time(self, dynamic_cores_per_stage = config) 
                est_reward = job.estimate_reward(est_time * self.vscale_params["queuefactor"])

                # Calculate costs based on the *average* number of cores we will use during our lifetime.
                # This may make us eager to break into expensive resources just now based on a prediction that the remainder of the job will run in similar circumstances and can fit on average.
                # As for the greedy scheme, the greed parameter moderates this desire to some degree.
                new_cores = new_splits * avgcores
                new_private_cores = min(new_cores, private_tier_free_cores)
                new_public_cores = new_cores - new_private_cores

                new_cores_cost = (new_private_cores * private_tier["cost"]) + (new_public_cores * public_tier["cost"])
                new_cores_cores *= est_time
                average_cost_per_split = float(new_cores_cost) / new_splits
                
                est_reward -= average_cost_per_split

                if self.debug:
                    print "Running with plan", config, "would yield reward", est_reward, "(average cost %g)" % average_cost_per_split

                if best_plan is None or est_reward > best_reward:
                    best_plan = config
                    best_reward = est_reward

            job.longterm_plan = best_plan
            return best_plan[0]

        def pick_dynamic_cores_ltadaptive():
            
            job = split.stage.job
            configs_so_far = tuple([rec["cores"] for rec in job.stage_configs])
            ideal_plan = self.ltadaptive_plans[configs_so_far]
            def guess_future_stage_cores(this_stage_cores):
                new_config = configs_so_far + (this_stage_cores,)
                if len(new_config) == 7:
                    return 1
                else:
                    return self.ltadaptive_plans[new_config]
            return pick_dynamic_cores_greedy_core([cores for cores in params.dynamic_core_choices if cores <= ideal_plan[job.current_stage]], future_dynamic_cores_fn = guess_future_stage_cores)

        def guess_job_plan(job):

            if self.vscale_algorithm == "greedy":
                plan = [c["cores"] for c in job.stage_configs]
                plan.extend([1] * (7 - len(plan)))
            elif self.vscale_algorithm == "longterm":
                if job.longterm_plan is None:
                    plan = self.candidate_configs[0][0]
                else:
                    plan = job.longterm_plan
            elif self.vscale_algorithm == "ltadaptive":
                history = [c["cores"] for c in job.stage_configs]
                plan = self.ltadaptive_plans[tuple(history)]
            elif self.vscale_algorithm == "constant":
                plan = self.vscale_params["plan"]
            else:
                plan = None

            return plan

        run_now = True
        could_run_now = True

        
        if self.nmachines[queueidx] is not None and sum([len(x) for x in self.active_splits_by_type[queueidx]]) == self.nmachines[queueidx]:
            # No machines available
            run_now = False
            could_run_now = False
        elif self.ncores is not None and self.running_cores + self.machine_specs[split.stage.stage_idx] > self.ncores:
            # No cores available
            run_now = False
            could_run_now = False

        if could_run_now:

            if split.stage.is_gather:
                cores = 1
            else:
                if self.ncores is not None:
                    cores = self.machine_specs[split.stage.stage_idx]
                else:
                    cores = self.machine_specs[queueidx]
            # Using dynamic vertical scaling?
            if cores is None:
                cores = split.stage.active_cores
            if cores is None:
                cores = pick_dynamic_cores()            

        if should_defer(cores):
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

        vm_startup_delay = params.vm_startup_delay if self.nmachines[queueidx] is None else 0

        if self.ncores is None:
            private_tier_free_cores = params.core_cost_tiers[0]["cores"] - self.active_cores_by_tier(0)
            if private_tier_free_cores >= cores:
                split.run_tier = 0
            else:
                split.run_tier = 1
            self.active_splits_by_type[queueidx][split.run_tier].append(split)
        else:
            self.running_cores += cores
        split.schedule_split(self, cores, vm_startup_delay)

        return True

    def release_split_resources(self, split):

        queue_idx = split.stage.queue_idx(self)

        if self.ncores is not None:
            self.running_cores -= self.machine_specs[split.stage.stage_idx]
        else:
            splits = self.active_splits_by_type[queue_idx][split.run_tier]
            splits.remove(split)

        self.try_run_next_split(queue_idx)

    def queue_next_arrival(self):
        
        arrival_time, event = self.arrival_process.next(self)
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

        state.release_split_resources(self.split)
        self.split.split_done(state)

next_job_id = 0
def fresh_job_id():
    global next_job_id
    ret = next_job_id
    next_job_id += 1
    return ret

# JobSplit keeps track of state that only applies to one split
class JobSplit:

    def __init__(self, stage, split_idx, created_time):

        self.stage = stage
        self.split_start_time = None
        self.split_finish_time = None
        self.split_idx = split_idx
        self.created_time = created_time

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

        queue_delay = state.now - self.created_time
        self.stage.job.total_queue_delay += queue_delay
        
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
        if self.active_cores is None and self.splits_scheduled == 0:
            self.job.stage_configs.append({"cores": cores})
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
        self.stage_configs = []
        self.longterm_plan = None
        self.total_queue_delay = 0.0
        self.cost = 0.0

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
                if isinstance(dynamic_cores_per_stage, numbers.Integral):
                    ret = dynamic_cores_per_stage
                else:
                    ret = dynamic_cores_per_stage[i]
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
        self.reward = params.reward(state.now - self.start_time, self.nrecords)
        if state.debug:
            print "Job", str(self), "done; credit", self.reward
        state.total_reward += self.reward
        self.actual_finish_time = state.now
        state.completed_jobs.append(self)

class NormalArrivalProcess:

    def __init__(self, mean_arrival = "3.0", mean_jobs = "3", jobs_var = "2", mean_records = "5", records_var = "1"):
        self.mean_arrival = float(mean_arrival)
        self.arrival_lambda = 1.0 / self.mean_arrival
        self.mean_jobs = float(mean_jobs)
        self.jobs_var = float(jobs_var)
        self.mean_records = float(mean_records)
        self.records_var = float(records_var)

    def get_arrival_lambda(self, state):
        return self.arrival_lambda

    def next(self, state):
        
        arrival_interval = random.expovariate(self.get_arrival_lambda(state))
        if arrival_interval == float("inf") or arrival_interval <= 0:
            arrival_interval = self.mean_arrival

        njobs = int(random.normalvariate(self.mean_jobs, self.jobs_var))
        if njobs <= 0:
            njobs = 1
        
        jobs = []
        for i in range(njobs):
            jobs.append(Job(random.normalvariate(self.mean_records, self.records_var)))

        return (arrival_interval, ArrivalEvent(jobs))

class WeekendArrivalProcess(NormalArrivalProcess):
    
    def __init__(self, period = "1000", weekdaymean = "2.5", weekendmean = "7.5", **kwargs):
        NormalArrivalProcess.__init__(self, **kwargs)
        self.period = int(period)
        self.weekday_lambda = 1.0 / float(weekdaymean)
        self.weekend_lambda = 1.0 / float(weekendmean)
        
    def get_arrival_lambda(self, state):
        interval = int(state.now) / self.period
        if interval % 2 == 1:
            return self.weekend_lambda
        else:
            return self.weekday_lambda

class UpdateAveragesEvent:

    def run(self, state):
        state.update_average_lengths()

