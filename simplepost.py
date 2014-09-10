#!/usr/bin/python

import httplib
import urllib

def post(host, port, address, params):

    params = urllib.urlencode(params)
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    conn = httplib.HTTPConnection(host, port, strict=False, timeout=5)
    conn.request("POST", address, params, headers)
    response = conn.getresponse()
    if response.status != 200:
        raise Exception("Request %s %s %s returned status %d" % (host, address, params, response.status))
    return response
