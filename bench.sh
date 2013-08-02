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
size[2]="10000"
size[3]="100000"
size[4]="100000"

arity[1]="2"
arity[2]="8"
arity[3]="8"
arity[4]="16"

mkdir auto-results;

    for nodes in 16
    do
        tail -$nodes all_machines > /home/ndiegues/machines
        for attempt in 1 
        do
            for ro in 2
            do
                for opt in 1
                do
		for sa in 1 2 3 4
		do
                bash btt-scripts/run-test.sh ${readPerc[$ro]} 60 ${size[$sa]} ${options[$opt]} ${arity[$sa]}
                cp results-radargun/test-result-results2/infinispan4_ispn_$nodes.csv auto-results/$nodes-${readPerc[$ro]}-${optStr[$opt]}-${size[$sa]}-${arity[$sa]}-$attempt.csv
		done
                done
            done
        done
    done
