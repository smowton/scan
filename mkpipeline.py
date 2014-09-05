#!/usr/bin/python

import sys
import string
import random

def mkpipeline(scripts_and_classes):

    # Take a list of scripts and make them into a driver script
    # that quotes each one in turn to temporary storage and runs
    # each in a different SCAN task

    print "TD=`mktemp -d`"

    # Write temporary scripts that will link each subsequent stage together:

    for (idx, (scriptname, classname)) in enumerate(scripts_and_classes[1:]):
        
        with open(scriptname, "r") as f:
            script = f.read()

        token = "EOF"
        while script.find(token) != -1:
            token = "%s%s" % (token, random.choice(string.ascii_letters))

        print "cat > $TD/stage%d << %s" % (idx + 2, token)
        sys.stdout.write(script)
        if idx < (len(scripts_and_classes) - 2):
            print "\n~/csmowton/scan/scansubmit.py --class=%s $TD/stage%d" % (scripts_and_classes[idx+2][1], idx + 3)
        print token
        print

    # Now do stage 1:

    with open(scripts_and_classes[0][0], "r") as f:
        sys.stdout.write(f.read())

    # ...and cue stage 2.

    print "\n~/csmowton/scan/scansubmit.py --class=%s $TD/stage2" % scripts_and_classes[1][1] 

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: mkpipeline.py script1 script2 [script3 ...]"
    sys.exit(1)

scripts_and_classes = [(s, "A") for s in sys.argv[1:]]
mkpipeline(scripts_and_classes)
    
