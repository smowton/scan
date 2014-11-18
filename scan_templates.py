
def templates():

    return {"hello_world": {"desc": "Hello World", 
                            "script": 'echo "%s" > /tmp/hello_world',
                            "classname": "gatk_rtc"},
            "gatk": {"desc": "Genome Analysis (GATK)", 
                     "script": "/root/scan/dummy_clients/start_gatk_pipeline.sh %s 1 /mnt/nfs /mnt/nfs/test_workdir /mnt/nfs/Queue-3.1-smowton.jar",
                     "classname": "gatk_queue_runner"}}

