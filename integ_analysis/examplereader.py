#!/usr/bin/python

# Simple example read client: test how often samples with attribute x also have attribute y. Attribute values are not examined.

import cqlscan
import sys

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: examplereader.py attrib1 attrib2"
	sys.exit(1)

(con, cursor) = cqlscan.cql_connect()

cursor.execute("select sampleid from observations where obskey = '%s'" % sys.argv[1])
samples = [r[0] for r in cursor]

total = len(samples)
count = 0

for s in samples:
	cursor.execute("select count(*) from observations where sampleid = '%s' and obskey = '%s'" % (s, sys.argv[2]))
	count += int(cursor.next()[0])

print ("%d/%d" % (count, total)), "samples with", sys.argv[1], "also have", sys.argv[2]
