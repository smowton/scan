#!/usr/bin/python

import simplepost
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--host", dest = "host", default = "localhost")
parser.add_argument("--port", dest = "port", default = 8080, type = int)
parser.add_argument("--class", dest = "classname", default = "A")
parser.add_argument("--fsreservation", dest = "fsreservation", default = "0")
parser.add_argument("--dbreservation", dest = "dbreservation", default = "0")
parser.add_argument("scriptname")

args = parser.parse_args()

if args.scriptname == "-":
    script = sys.stdin.read()
else:
    with open(args.scriptname, "r") as f:
        script = f.read()

simplepost.post(args.host, args.port, "/addworkitem", { "classname": args.classname, 
                                                        "fsreservation": args.fsreservation, 
                                                        "dbreservation": args.dbreservation,
                                                        "cmd": script })

