
def getclasses(gatk_ssh_username = "user", gatk_init_cores = "1", gatk_init_mem = "4096"):

    return [{"name": "gatk_%s" % x, 
             "user": gatk_ssh_username, 
             "respath": "/home/user/csmowton/scan/getres.py",
             "init_hwspec": {"cores": int(gatk_init_cores), "memory": int(gatk_init_mem)}}
            for x in ["rtc", "ir", "ir_gather", "br", "pr", "pr_gather", "ug", "ug_gather", "vf", "vf_gather", "ve", "queue_runner"]]

