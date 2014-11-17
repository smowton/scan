#!/usr/bin/python

import cherrypy
import sys
import threading
import json
import await_server
import socket
import simplepost

if len(sys.argv) < 2:
    print >>sys.stderr, "Usage: blocking_delete.py classname [wid]"
    sys.exit(1)

if len(sys.argv) >= 3:
    wid = int(sys.argv[2])
else:
    wid = None

# This implements a blocking delete operation: we ask localhost:8080 to delete some worker,
# and use our own tiny server to catch the callback when it goes away.
# Hopefully in due time this will be replaced by asynchrony support within CELAR.

def unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port

port = unused_port()

def cherrypy_thread():

    class CallbackCatcher:

        @cherrypy.expose
        def ping(self, echo):
            return json.dumps({"echo": echo})

        @cherrypy.expose
        def callback(self, classname, wid, address):
            print "%s:%s:%s" % (classname, address, wid)
            cherrypy.engine.exit()

    catcher = CallbackCatcher()
    cherrypy.config.update({'log.screen': False})
    cherrypy.server.socket_port = port
    cherrypy.quickstart(catcher)

t = threading.Thread(target=cherrypy_thread)
t.start()

await_server.await_server(host="localhost", port=port, tries=20, delay=0.5)

# Make the delete request:

if wid is not None:
    desc = str(wid)
else:
    desc = "(any)"

print >>sys.stderr, "Listening on port %d; requesting worker %s deletion from class %s" % (port, desc, sys.argv[1])

post_params = {"classname": sys.argv[1], "callbackaddress": "http://localhost:%d/callback" % port}
if wid is not None:
    post_params["wid"] = str(wid)
simplepost.post(host="localhost", port=8080, address="/delworker", params=post_params)

