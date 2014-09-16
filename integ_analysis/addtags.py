#!/usr/bin/python

import sys
import cqlscan

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: addtags.py sampleid key1=val1 [key2=val2 ...]"
    sys.exit(1)

attribs = dict()

for t in sys.argv[2:]:
    bits = t.split("=")
    if len(bits) != 2:
        raise Exception("Tag %s not given in key=value form" % t)
    attribs[bits[0]] = bits[1]

tok = cqlscan.cql_connect()
sampleid = cqlscan.cql_add_sample(tok, attribs, sys.argv[1])
cqlscan.cql_close(tok)

print "Added tags to", sampleid
