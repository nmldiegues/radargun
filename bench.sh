#!/bin/bash

#nova list | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' > all_machines

readPerc[1]="25"
readPerc[2]="50"
readPerc[3]="80"
readPerc[4]="100"
readPerc[5]="0"
readPerc[6]="1"
readPerc[7]="5"
readPerc[8]="10"

workload[1]="a"
workload[2]="b"
workload[3]="c"
workload[4]="d"
workload[5]="e"
workload[6]="f"

options[1]="true true true false"
options[2]="false false false false"
options[3]="true true true false"
options[4]="true true true false"
options[5]="true true true true"
options[6]="false true true false"
options[7]="true false true false"
options[8]="true true false false"

optStr[1]="col"
optStr[2]="ran"
optStr[3]="minuetPartial"
optStr[4]="minuetFull"
optStr[5]="colList"
optStr[6]="col-norepl"
optStr[7]="col-nocol"
optStr[8]="col-noghost"

emulation[1]="none"
emulation[2]="none"
emulation[3]="minuetPartial"
emulation[4]="minuetFull"
emulation[5]="none"
emulation[6]="none"
emulation[7]="none"
emulation[8]="none"

size[1]="100000"
size[2]="100000"
size[3]="100000"
size[4]="100000"
size[5]="100000"
size[6]="100000"
size[7]="100000"

arity[1]="2"
arity[2]="4"
arity[3]="8"
arity[4]="12"
arity[5]="32"
arity[6]="64"
arity[7]="128"


keyRange[1]="100000"
keyRange[2]="100000"
keyRange[3]="100000"
keyRange[4]="100000"
keyRange[5]="100000"
keyRange[6]="100000"
keyRange[7]="100000"

ro=1

    for attempt in 1
    do
        echo "going for attempt $attempt"
        for nodes in 60 #8 16 24 32 40 48 56 64 80 100
        do
        head -$nodes all_machines > /home/$USER/machines

                echo "going for nodes $nodes"
            for work in 1 #2 3 4
            do
                echo "going for ro $ro"
                for opt in 1 #2 6 7 8
                do
                echo "going for opt $opt"
                for sa in 4 5 6 7
                do
                echo "going for sa $sa"
                echo "bash btt-scripts/run-test.sh ${readPerc[$ro]} ${size[$sa]} ${keyRange[$sa]} ${options[$opt]} ${arity[$sa]} ${emulation[$opt]} ${workload[$work]}"
                bash btt-scripts/run-test.sh ${readPerc[$ro]} ${size[$sa]} ${keyRange[$sa]} ${options[$opt]} ${arity[$sa]} ${emulation[$opt]} ${workload[$work]}
                mv results-radargun/test-result-results2/infinispan4_ispn_$nodes.csv auto-results/$nodes-${readPerc[$ro]}-${optStr[$opt]}-${emulation[$opt]}-${size[$sa]}-${keyRange[$sa]}-${arity[$sa]}-${workload[$work]}-$attempt.csv
                rc=$?
                if [[ $rc != 0 ]] ; then
                    echo "Error within: bash btt-scripts/run-test.sh ${readPerc[$ro]} ${size[$sa]} ${keyRange[$sa]} ${options[$opt]} ${arity[$sa]} ${emulation[$opt]} ${workload[$work]}" >> auto-results/error.out
                fi
                done
                done
            done
        done
    done

