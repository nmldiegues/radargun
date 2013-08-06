#!/bin/bash

nova list | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' > all_machines

readPerc[1]="25"
readPerc[2]="50"
readPerc[3]="80"
readPerc[4]="100"

options[1]="true true true true"
options[2]="false false false false"

optStr[1]="col"
optStr[2]="ran"

size[1]="1000"
size[2]="1000"
size[3]="10000"
size[4]="10000"
size[5]="100000"
size[6]="100000"

arity[1]="2"
arity[2]="3"
arity[3]="8"
arity[4]="16"
arity[5]="8"
arity[6]="16"

echo "starting"
    for attempt in 1 2
    do
	echo "going for attempt $attempt"
        for nodes in 32
        do
        tail -$nodes all_machines > /home/ndiegues/machines

		echo "going for nodes $nodes"
            for ro in 1 2 3 4
            do
		echo "going for ro $ro"
                for opt in 1
                do
		echo "going for opt $opt"
		for sa in 1 2 5 6
		do
		echo "going for sa $sa"
		echo "bash btt-scripts/run-test.sh ${readPerc[$ro]} 60 ${size[$sa]} ${size[$sa]} ${options[$opt]} ${arity[$sa]}"
                bash btt-scripts/run-test.sh ${readPerc[$ro]} 60 ${size[$sa]} ${size[$sa]} ${options[$opt]} ${arity[$sa]}
                mv results-radargun/test-result-results2/infinispan4_ispn_$nodes.csv auto-results/$nodes-${readPerc[$ro]}-${optStr[$opt]}-${size[$sa]}-${arity[$sa]}-$attempt.csv
		rc=$?
		if [[ $rc != 0 ]] ; then
		    echo "Error within: bash btt-scripts/run-test.sh ${readPerc[$ro]} 60 ${size[$sa]} ${size[$sa]} ${options[$opt]} ${arity[$sa]}"
		    exit $rc
		fi
		done
                done
            done
        done
    done
