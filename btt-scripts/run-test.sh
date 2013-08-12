#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

echo "loading environment..."
. ${WORKING_DIR}/environment.sh

READ_ONLY_PERC=$1
DURATION=$2
KEYS_SIZE=$3
KEYS_RANGE=$4
REPLICATION_DEGREES=$5
COLOCATION=$6
GHOST_READS=$7
THREAD_MIGRATION=$6
INTRA_NODE_CONC=$8
LOWER_BOUND=$9
NR_NODES_TO_USE=`wc -l /home/ndiegues/machines | awk '{print $1}'`
EST_DURATION="1"

BENC_DEFAULT="-distributed -c $NR_NODES_TO_USE -k $KEYS_SIZE -kr $KEYS_RANGE -d $DURATION -ro $READ_ONLY_PERC -t $THREAD_MIGRATION -r $REPLICATION_DEGREES -l $COLOCATION -g $GHOST_READS -i $INTRA_NODE_CONC -b $LOWER_BOUND"

echo "============ INIT BENCHMARKING ==============="

clean_master
# kill_java ${CLUSTER}
# clean_slaves ${CLUSTER}

echo "============ STARTING BENCHMARKING ==============="

#lp => locality probability= 0 15 50 75 100
#wrtPx => write percentage== 0 10
#rdg => replication degree== 1 2 3

${JGRP_GEN} -sequencer -toa -tcp

echo "After generating jgroups"

for owner in 1; do
#for l1 in -l1-rehash none; do
#for wrtPx in 0 10; do
#for rdg in 1 2 3; do
#for keys in 1000 8000 16000; do
#for bfFp in 0.01 0.10; do

#${ISPN_GEN} ${ISPN_DEFAULT} -num-owner ${owner}
echo "============= Before generating benchmark ==========="
${BENC_GEN} ${BENC_DEFAULT}
echo "=============== Going to run TEST ================"
run_test ${NR_NODES_TO_USE} "results2" ${EST_DURATION} ${CLUSTER}
killall -9 java
done
#done
#done

echo "============ FINISHED BENCHMARKING ==============="

exit 0
