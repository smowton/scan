
def getclasses():

    # Start with rough approximations of task performance.
    # time_reward: (target time in hours, reward per hour above/below the target)
    # size_time: (constant time in hours, hours per unit estimated job size)
    # thread_time: Proportion scalable.

    gatkclasses = ["rtc", "ir", "ir_gather", "br", "pr", "pr_gather", "ug", "ug_gather", "vf", "vf_gather", "ve", "queue_runner"]
    singlethread_classes = ["ir", "vf"]
    quick_classes = ["vf"]
    quick_classes.extend([x for x in gatkclasses if "_gather" in gatkclasses])

    return {k: {"description": "GATK class %s" % k, 
                "time_reward": (1.0, 1.0) if k not in quick_classes else (0.1, 1.0), 
                "size_time": (0.1, 0.5) if k not in quick_classes else (0.05, 0.05),
                "thread_time": 0.8 if k not in singlethread_classes else 0.0} 
            for k in gatkclasses}


