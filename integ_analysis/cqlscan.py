
import cql
import uuid

def cql_connect(host = "localhost", keyspace = "scan"):

	con = cql.connect(host = host, keyspace = keyspace)
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

def cql_close(cc):
	
	con, cursor = cc
	cursor.close()
	con.close()
