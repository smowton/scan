
def getclasses():

    return [{"name": "gatk_%s" % x, 
             "user": "user", 
             "respath": "/home/user/csmowton/scan/getres.py",
             "init_hwspec": {"cores": 1, "memory": 4096}}
            for x in ["rtc", "ir", "br", "pr", "ug", "vf", "ve"]]

