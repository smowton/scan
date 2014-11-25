#!/bin/bash

cat $1 | head -n 167 > /tmp/header
cat $1 | tail -n +168 | head -n 475 > /tmp/mt
tail -n +643 $1 | cat /tmp/header - /tmp/mt > $1.new
mv $1.new $1
rm $1.idx
java -cp /mnt/nfs/Queue-3.1-smowton.jar org.broadinstitute.sting.gatk.CommandLineGATK -R /mnt/nfs/Homo_sapiens.GRCh37.56.dna.chromosomes_and_MT.fa -T ValidateVariants --variant $1
