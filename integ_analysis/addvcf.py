#!/usr/bin/python

import cqlscan
import sys
import json
import httplib

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: addvcf.py in.vcf sampleid"
	sys.exit(1)

with open(sys.argv[1], "r") as vcf:

	attribdict = dict()
        conn = httplib.HTTPConnection("rest.ensembl.org", 80, strict=False, timeout=5)

	for vcfl in vcf:
		if vcfl.startswith("#"):
			continue
		bits = [x.strip() for x in vcfl.split("\t")]
		# Key: chrom-position-oldseq-newseq
		key = "\t".join([bits[0], bits[1], bits[3], bits[4]])
		# Val: the rest of the fields
		val = "\t".join(bits[5:])
		attribdict[key] = val

                # Find proteins that may be broken by this mutation by querying ensembl:

                url = "/overlap/region/human/%s:%s-%s?feature=gene;content-type=application/json" % (bits[0], bits[1], bits[1])
                print "Check", url
                conn.request("GET", url)
                response = conn.getresponse()
                if response.status != 200:
                        print >>sys.stderr, "HTTP %d getting %s" % (response.status, url)
                        continue
                ret = response.read()
                genes = json.loads(ret)
                for gene in genes:
                        try:
                                if gene["biotype"] == "protein_coding":
                                        attribdict["mutated_%s" % str(gene["external_name"])] = "true"
                        except KeyError:
                                pass

tok = cqlscan.cql_connect()
sampleid = cqlscan.cql_add_sample(tok, attribdict, sys.argv[2])
cqlscan.cql_close(tok)

print "Added sample", sampleid
