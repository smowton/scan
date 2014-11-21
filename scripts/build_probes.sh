#!/bin/bash
# Fetch the json.org dependency, and build the SCAN JC probes

# Get probe dependency:
cd /tmp
git clone https://github.com/douglascrockford/JSON-java.git
mkdir -p org/json
mv JSON-java/* org/json/
javac org/json/*.java org/json/zip/*.java

# Build the JCatascopia probe:
cd ~/scan/jc_probes
javac *.java -cp /usr/local/bin/JCatascopiaAgentDir/JCatascopia-Agent-0.0.1-SNAPSHOT.jar:/tmp
cp *.class /tmp
cd /tmp
jar cvf ~/scan/jc_probes/ScanProbe.jar *.class org
