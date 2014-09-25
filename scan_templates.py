
def templates():

    return {"echotest_linux": {"desc": "Genome Analysis (GATK)", 
                                "script": "N=%s; echo $N > /tmp/$N",
                                "classname": "linux"},
            "echotest_win": {"desc": "Write some temporary files (Windows)", 
                             "script": "N=%s; echo $N > /tmp/$N",
                             "classname": "windows"}}

