

import sys
try:
	import cql
except:
	print >>sys.stderr, "Failed to load cql module; integrated results not available"
import uuid

def cql_connect(host = "localhost", keyspace = "scan"):

	con = cql.connect(host = host, keyspace = keyspace, cql_version = "3.0.0")
	cursor = con.cursor()
	return (con, cursor)

def cql_add_sample(cc, attribs, sampleid):

        sampleid = int(sampleid)
	con, cursor = cc
	for (k, v) in attribs.iteritems():
		obsid = (uuid.uuid4().int) & ((1 << 63) - 1)
		textattribs = "( %d, %d, '%s', '%s' )" % (obsid, sampleid, k, v)
		q = "insert into observations (obsid, sampleid, obskey, obsval) values " + textattribs
		if not cursor.execute(q):
			raise Exception("Query " + q + " failed")
	return sampleid

def cql_init_db(cc):

	con, cursor = cc
	if not cursor.execute("create keyspace scan with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"):
		raise Exception("Create keyspace error")
	if not cursor.execute("create table scan.observations ( obsid bigint primary key, obskey text, sampleid bigint, obsval text )"):
		raise Exception("Create table error")

def cql_close(cc):
	
	con, cursor = cc
	cursor.close()
	con.close()
