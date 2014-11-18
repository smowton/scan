import cqlscan
import threading
import websupport
import cherrypy
import sys
import os
import math
import cStringIO
import contextlib

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt_avail = True
except Exception as e:
    print >>sys.stderr, "Matplotlib not available"
    plt_avail = False

def chrom_order((lhsk, lhsv), (rhsk, rhsv)):

    if type(lhsk) == type(rhsk):
        return cmp(lhsk, rhsk)
    elif type(lhsk) == int:
        return -1
    else:
        return 1

class ResultViewer:

    def __init__(self):
        
        host_key = 'SCAN_DB_HOST'
        if host_key not in os.environ:
            dbhost = "localhost"
        else:
            dbhost = os.environ[host_key]
        try:
            self.conn, self.cursor = cqlscan.cql_connect(host = dbhost)
        except Exception as e:
            print >>sys.stderr, "Failed to connect to Cassandra, results unavailable"
            self.conn = None
            self.cursor = None
        self.lock = threading.Lock()

    def getvars(self):
        # Find all defined attributes that measure
        # (a) Mutations at [site]
        # (b) Mutations in [protein]
        # (c) Protein expression intensity

        if self.cursor is None:
            return ([], [], [])

        with self.lock:

            self.cursor.execute("select obskey from observations")
            keys = [r[0] for r in self.cursor]

        genome_muts = []
        protein_muts = []
        expressions = []

        for k in keys:
            
            if k is None:
                continue

            if k.find("\t") != -1:
                bits = k.split("\t")
		try:
			sk = int(bits[0])
			sk = "A%04d" % sk
		except ValueError:
			sk = "B%s" % bits[0]
                genome_muts.append({"desc": "%s:%s" % (bits[0], bits[1]), "name": k, "sortkey": sk})
            elif k.find("-intensity") != -1:
                expressions.append({"desc": k[:-len("-intensity")], "name": k, "sortkey": k})
            elif k.find("mutated_") != -1:
                protein_muts.append({"desc": k[len("mutated_"):], "name": k, "sortkey": k})

        getkey = lambda x: x["sortkey"]

        return (sorted(genome_muts, key=getkey)[:20], sorted(protein_muts, key=getkey)[:20], sorted(expressions, key=getkey)[:20])

    @cherrypy.expose
    def index(self):
        
        genome_muts, protein_muts, expressions = self.getvars()
        combo_items = ([{"desc": "--Protein mutations--", "name": "invalid"}]
                       + protein_muts
                       + [{"desc": "--Protein expression levels--", "name": "invalid"}]
                       + expressions
                       + [{"desc": "--Gene mutations--", "name": "invalid"}]
                       + genome_muts)

        return """<html><body><h3>SCAN Prototype Result Viewer</h3>
<form method="get" action="/results/correlate">
<p>Correlate %s with %s</p>
<p>Or, find top correlates for %s</p>
<p><input type="submit"/>
</form><p><a href="/results/mutation_graph">Show mutation histogram</a></p>
</body></html>""" % (websupport.mkcombo("single_1", combo_items),
	             websupport.mkcombo("single_2", combo_items),
        	     websupport.mkcombo("multi", combo_items))

    def correlate_one(self, x, y):

        pass

    def correlate_multi(self, var):

        genome_muts, protein_muts, expressions = self.getvars()

    @cherrypy.expose
    def mutation_graph(self):

        if not plt_avail:
            return "<html><body><p>Check matplotlib is installed</p></body></html>"

        with self.lock:
                
            self.cursor.execute("select obskey from observations")
            keys = [r[0] for r in self.cursor]

        chroms = dict()
        
        for key in keys:

            bits = key.split("\t")
            try:
                chrom = int(bits[0])
            except:
                chrom = bits[0]
            if chrom not in chroms:
                chroms[chrom] = []
            chroms[chrom].append(int(bits[1]))

        keys = list(chroms.keys())
        for key in keys:
            chroms[key] = sorted(chroms[key])

        gridwidth = math.ceil(math.sqrt(len(chroms)))
        gridheight = (len(chroms) + (gridwidth - 1)) / gridwidth

        plt.figure(figsize = (12, 12), dpi=72)

        chroms = sorted(chroms.items(), cmp = chrom_order)

        for i, (chrom, vals) in enumerate(chroms):
            
            plt.subplot(gridwidth, gridheight, i + 1)
            plt.locator_params(nbins = 5)
            plt.tick_params(labelsize = 10)
            plt.hist(vals)
            plt.title(chrom)
            plt.tight_layout()

        cherrypy.response.headers["Content-Type"] = "image/png"

        buf = cStringIO.StringIO() 
        with contextlib.closing(buf):
            plt.savefig(buf, format="png")
            return buf.getvalue()

    @cherrypy.expose
    def correlate(self, single_1, single_2, multi):

        if single_1 == "null":
            return correlate_multi(multi)
        else:
            return correlate_one(single_1, single_2)

