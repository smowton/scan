#!/usr/bin/env python

import json
import sys
import subprocess

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: okeanos_commission_cluster.sh cluster_defn.json cluster_machines.json"
	sys.exit(1)

with open(sys.argv[1], "r") as f:
	defn = json.load(f)

# Find the network we will need:

nets = json.loads(subprocess.check_output(["kamaki", "network", "list", "-j"]))
private_nets = filter(lambda x: x["_2_public"] == "( private )", nets)

if len(private_nets) == 0:
	print >>sys.stderr, "No private nets found"
	sys.exit(1)

if len(private_nets) > 1:
	print >>sys.stderr, "Multiple private nets found, using the first"

network_id = int(private_nets[0]["_0_id"])
print "Using network ID", network_id

# Check the network is empty at the moment

print "Check for already-used IPs..."

active_servers = json.loads(subprocess.check_output(["kamaki", "server", "list", "-j"]))
free_ips = ["192.168.0.%d" % i for i in range(2, 255)]

for server in active_servers:

	server_info = json.loads(subprocess.check_output(["kamaki", "server", "info", str(server["id"]), "-j"]))
	for att in server_info["attachments"]:

		if att["network_id"] == str(network_id):
			ip = att["ipv4"]
			if ip != "192.168.0.1":
				print >>sys.stderr, "Warning: IP", ip, "still active"
				free_ips.remove(ip)

print "Check needed flavours..."

ram = 2048
disk = 20

spec_to_flavour = {}

flavours = json.loads(subprocess.check_output(["kamaki", "flavor", "list", "-j"]))
for flavour in flavours:

	name = flavour["name"]
	if not name.endswith("drbd"):
		continue

	if name.find("C") == -1 or name.find("R") == -1 or name.find("D") == -1:
		print >>sys.stderr, "Warning: ignore flavour", flavour, "with weird name format"
		continue

	name = name.replace("C", "")
	name = name.replace("R", "|")
	name = name.replace("D", "|")
	name = name.replace("drbd", "")
	
	(fcores, fram, fdisk) = tuple([int(x) for x in name.split("|")])

	spec_to_flavour[(fcores, fram, fdisk)] = flavour["id"]

for tier in defn:

	if (tier["cores"], ram, disk) not in spec_to_flavour:
		print >>sys.stderr, "Need unlisted flavour", (tier["cores"], ram, disk)
		sys.exit(1)		

worker_idx = 1

machines = []
machines_out = open(sys.argv[2], "w")

for tier in defn:

	count = tier["count"]
	cores = tier["cores"]
	flavour = spec_to_flavour[(cores, ram, disk)]

	for i in range(count):

                ip = free_ips.pop()
                name = "Worker-%d-%d-cores" % (worker_idx, cores)

		new_server = json.loads(subprocess.check_output(["kamaki", "server", "create", "-j", "--name=" + name, 
			"--flavor-id=%d" % flavour, "--image-id=fe31fced-a3cf-49c6-b43b-f58f5235ba45", 
			"--network=%d,%s" % (network_id, ip), 
			"-p", "/home/csmowton/.ssh/celarcluster.pub,/home/user/.ssh/authorized_keys,user,users,0644"]))

		machines.append({"name": name, "id": new_server["id"], "cores": cores, "passwd": new_server["adminPass"], "ip": ip})

                worker_idx += 1

json.dump(machines, machines_out)
machines_out.close()


