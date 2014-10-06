
def getclasses():

    return [{"name": "gatk_%s" % x, 
             "user": "user", "respath": 
             "/home/user/csmowton/scan/getres.py"}
            for x in ["rtc", "ir", "br", "pr", "ug", "vf", "ve"]]

