#!/usr/bin/env python

import json
import sys

nodes = json.load(sys.stdin)

uniq = dict()

for node in nodes:
    uniq[node["name"]] = node

json.dump(uniq.values(), sys.stdout)

