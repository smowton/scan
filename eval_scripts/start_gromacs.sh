#!/bin/bash

RUNID=`uuidgen`

curl --upload-file /scratch/csmowton/scan_testdata/gromacs/to_upload/EM.top http://snf-665817.vm.okeanos.grnet.gr:8080/dfsput\?path=$RUNID/EM.top
curl --upload-file /scratch/csmowton/scan_testdata/gromacs/to_upload/EM.tpr http://snf-665817.vm.okeanos.grnet.gr:8080/dfsput\?path=$RUNID/EM.tpr
curl --upload-file /scratch/csmowton/scan_testdata/gromacs/to_upload/posre.itp http://snf-665817.vm.okeanos.grnet.gr:8080/dfsput\?path=$RUNID/posre.itp

