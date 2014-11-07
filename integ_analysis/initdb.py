#!/usr/bin/python

import cqlscan
import sys

if len(sys.argv) < 2:
	print >>sys.stderr, "Usage: initdb.py hostname"
	sys.exit(1)

cc = cqlscan.cql_connect(sys.argv[1], keyspace = None)
cqlscan.cql_init_db(cc)
cqlscan.cql_close(cc)


