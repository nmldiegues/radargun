#!/bin/bash

nova list | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' > all_machines

algs[1]="gmu"
algs[2]="twm"

benchmarks[1]="ycsb"
benchmarks[2]="vacation"
benchmarks[3]="micro"

params[1]="50 5 8"
params[2]="50 16"
params[3]="50 5 8"

mkdir auto-results;

for benchmark in 1 2 3
do
    for nodes in 2 4 #10 20 30 40 50 60 70 80
    do
        tail -$nodes all_machines > /home/ndiegues/machines
        for attempt in 1 2 #3
        do
            for alg in 1 2
            do
                bash toggle_${algs[$alg]}
                bash ${benchmarks[$benchmark]}-scripts/run-test.sh ${params[$benchmark]}
                cp results-radargun/test-result-results2/infinispan4_ispn_$nodes.csv auto-results/${benchmarks[$benchmark]}-${algs[$alg]}-$nodes-$attempt.csv
            done
        done
    done
done

