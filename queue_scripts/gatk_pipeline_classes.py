
def getclasses():

    return [{"name": "gatk_%s" % x, 
             "user": "user", 
             "respath": "/home/user/csmowton/scan/getres.py",
             "init_hwspec": {"cores": 1, "memory": 4096}}
            for x in ["rtc", "ir", "ir_gather", "br", "pr", "pr_gather", "ug", "ug_gather", "vf", "vf_gather", "ve"]]

