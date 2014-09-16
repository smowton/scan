
import cql
import uuid

def cql_connect(host = "localhost", keyspace = "scan"):

	con = cql.connect(host = host, keyspace = keyspace)
	cursor = con.cursor()
	return (con, cursor)

def cql_add_sample(cc, attribs):

	con, cursor = cc
	newid = (uuid.uuid4().int) & ((1 << 63) - 1)
	for (k, v) in attribs.iteritems():
		obsid = (uuid.uuid4().int) & ((1 << 63) - 1)
		textattribs = "( %d, %d, '%s', '%s' )" % (obsid, newid, k, v)
		q = "insert into observations (obsid, sampleid, obskey, obsval) values " + textattribs
		if not cursor.execute(q):
			raise Exception("Query " + q + " failed")
	return newid

def cql_add_sample_tags(cc, attribs, tags):

	for t in tags:
		bits = t.split("=")
		if len(bits) != 2:
			raise Exception("Tag %s not given in key=value form" % t)
		attribs[t[0]] = t[1]

	return cql_add_sample(cc, attribs)

def cql_close(cc):
	
	con, cursor = cc
	cursor.close()
	con.close()
