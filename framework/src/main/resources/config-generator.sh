#!/bin/bash

WORKING_DIR=`cd $(dirname $0); cd ..; pwd`

DEST_FILE=${WORKING_DIR}/conf/benchmark.xml

PARTIAL_REPLICATION="false"
NO_CONTENTION="false"
NUMBER_OF_KEYS="1000"
SIZE_OF_VALUE="1000"
NUMBER_OF_THREADS="2"
#in seconds
SIMULATION_TIME="300"
WRITE_OP_PERCENTAGE="50"
WRITE_TX_PERCENTAGE="100"
LOWER_BOUND_OPERATIONS="10"
UPPER_BOUND_OPERATIONS="10"
COORDINATION_EXEC_TX="true"
CACHE_CONFIG_FILE="ispn.xml"
GET_KEYS=""

help_and_exit(){
echo "usage: ${0} <options>"
echo "options:"
echo "  -dest-file <value>               the file path where the generated configuration will be written"
echo "                                   default: ${DEST_FILE}"
echo ""
echo "  -simul-time <value>              simulation time (in seconds)"
echo "                                   default: ${SIMULATION_TIME}"
echo ""
echo "  -nr-keys <value>                 number of keys"
echo "                                   default: ${NUMBER_OF_KEYS}"
echo ""
echo "  -value-size <value>              the size of the value of each key (in bytes)"
echo "                                   default: ${SIZE_OF_VALUE}"
echo ""
echo "  -nr-thread <value>               the number of threads executing transactions in each node"
echo "                                   default: ${NUMBER_OF_THREADS}"
echo ""
echo "  -write-op-percentage <value>     percentage of write operation in a write transactions (0 to 100)"
echo "                                   default: ${WRITE_OP_PERCENTAGE}"
echo "  -write-tx-percentage <value>     percentage of write transactions (0 to 100)"
echo "                                   default: ${WRITE_TX_PERCENTAGE}"
echo ""
echo "  -config <value>                  the path for the configuration of the cache"
echo "                                   default: ${CACHE_CONFIG_FILE}"
echo ""
echo "  -min-op <value>                  minimum number of operations to be executed per transaction"
echo "                                   default: ${LOWER_BOUND_OPERATIONS}"
echo ""
echo "  -max-op <value>                  maximum number of operations to be executed per transaction"
echo "                                   default: ${UPPER_BOUND_OPERATIONS}"
echo ""
echo "  -no-coordinator-participation    the coordinator doesn't executes transactions"
echo "                                   default: coordinator execute transactions"
echo ""
echo "  -no-contention                   each thread has it owns keys and it has no conflicts between then"
echo "                                   default: contention can happen"
echo ""
echo "  -distributed                     set the configuration to use distributed mode"
echo "                                   default: is set to replicated mode"
echo ""
echo "  -passive-replication             set the configuration to use passive replication"
echo "                                   default: use a default scheme"
echo ""
echo "  -get-keys                        save the keys (and their values) in the end of the benchmark"
echo ""
echo "  -h                               show this message and exit"
exit 0
}

while [ -n $1 ]; do
case $1 in
  -h) help_and_exit;;
  -dest-file) DEST_FILE=$2; shift 2;;
  -simul-time) SIMULATION_TIME=$2; shift 2;;
  -nr-keys) NUMBER_OF_KEYS=$2; shift 2;;
  -value-size) SIZE_OF_VALUE=$2; shift 2;;
  -nr-thread) NUMBER_OF_THREADS=$2; shift 2;;
  -write-op-percentage) WRITE_OP_PERCENTAGE=$2; shift 2;;
  -write-tx-percentage) WRITE_TX_PERCENTAGE=$2; shift 2;;
  -config) CACHE_CONFIG_FILE=$2; shift 2;;
  -min-op) LOWER_BOUND_OPERATIONS=$2; shift 2;;
  -max-op) UPPER_BOUND_OPERATIONS=$2; shift 2;;
  -no-coordinator-participation) COORDINATION_EXEC_TX="false"; shift 1;;
  -no-contention) NO_CONTENTION="true"; shift 1;;
  -distributed) PARTIAL_REPLICATION="true"; shift 1;;
  -passive-replication) PASSIVE_REPLICATION="true"; shift 1;;
  -get-keys) GET_KEYS=1; shift 1;;
  -*) echo "WARNING: unknown option '$1'. It will be ignored" >&2; shift 1;;
  *) break;;
esac
done

let SIMULATION_TIME=${SIMULATION_TIME}*1000000000

echo "Writing configuration to ${DEST_FILE}"

echo "<bench-config>" > ${DEST_FILE}

echo "   <master" >> ${DEST_FILE}
echo "         bindAddress=\"\${127.0.0.1:master.address}\"" >> ${DEST_FILE}
echo "         port=\"\${21031:master.port}\"/>" >> ${DEST_FILE}

echo "   <benchmark" >> ${DEST_FILE}
echo "         initSize=\"\${10:Islaves}\"" >> ${DEST_FILE}
echo "         maxSize=\"\${10:slaves}\"" >> ${DEST_FILE}
echo "         increment=\"1\">" >> ${DEST_FILE}

echo "      <DestroyWrapper" >> ${DEST_FILE}
echo "            runOnAllSlaves=\"true\"/>" >> ${DEST_FILE}

echo "      <StartCluster" >> ${DEST_FILE}
echo "            staggerSlaveStartup=\"true\"" >> ${DEST_FILE}
echo "            delayAfterFirstSlaveStarts=\"5000\"" >> ${DEST_FILE}
echo "            delayBetweenStartingSlaves=\"1000\"/>" >> ${DEST_FILE}

echo "      <ClusterValidation" >> ${DEST_FILE}
echo "            passiveReplication=\"${PASSIVE_REPLICATION}\"" >> ${DEST_FILE}
echo "            partialReplication=\"${PARTIAL_REPLICATION}\"/>" >> ${DEST_FILE}

echo "      <ClearCluster />" >> ${DEST_FILE}

echo "      <WebSessionWarmup" >> ${DEST_FILE}
echo "            noContentionEnabled=\"${NO_CONTENTION}\"" >> ${DEST_FILE}
echo "            numberOfKeys=\"${NUMBER_OF_KEYS}\"" >> ${DEST_FILE}
echo "            sizeOfValue=\"${SIZE_OF_VALUE}\"" >> ${DEST_FILE}
echo "            numOfThreads=\"${NUMBER_OF_THREADS}\"/>" >> ${DEST_FILE}

echo "      <WebSessionBenchmark" >> ${DEST_FILE}
echo "            perThreadSimulTime=\"${SIMULATION_TIME}\"" >> ${DEST_FILE}
echo "            opsCountStatusLog=\"5000\"" >> ${DEST_FILE}
echo "            numberOfKeys=\"${NUMBER_OF_KEYS}\"" >> ${DEST_FILE}
echo "            sizeOfValue=\"${SIZE_OF_VALUE}\"" >> ${DEST_FILE}
echo "            numOfThreads=\"${NUMBER_OF_THREADS}\"" >> ${DEST_FILE}
echo "            writeOperationPercentage=\"${WRITE_OP_PERCENTAGE}\"" >> ${DEST_FILE}
echo "            writeTransactionPercentage=\"${WRITE_TX_PERCENTAGE}\"" >> ${DEST_FILE}
echo "            lowerBoundOp=\"${LOWER_BOUND_OPERATIONS}\"" >> ${DEST_FILE}
echo "            upperBoundOp=\"${UPPER_BOUND_OPERATIONS}\"" >> ${DEST_FILE}
echo "            coordinatorParticipation=\"${COORDINATION_EXEC_TX}\"" >> ${DEST_FILE}
echo "            noContentionEnabled=\"${NO_CONTENTION}\"/>" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE\" />" >> ${DEST_FILE}

if [ -n "${GET_KEYS}" ]; then
echo "      <GetKeys/>" >> ${DEST_FILE}
fi

echo "      <CsvReportGeneration/>" >> ${DEST_FILE}

echo "   </benchmark>" >> ${DEST_FILE}

echo "   <products>" >> ${DEST_FILE}

echo "      <infinispan4>" >> ${DEST_FILE}

echo "         <config name=\"${CACHE_CONFIG_FILE}\"/>" >> ${DEST_FILE}

echo "      </infinispan4>" >> ${DEST_FILE}

echo "   </products>" >> ${DEST_FILE}

echo "   <reports>" >> ${DEST_FILE}

echo "      <report name=\"Reports\" />" >> ${DEST_FILE}

echo "   </reports>" >> ${DEST_FILE}

echo "</bench-config>" >> ${DEST_FILE}

echo "Finished!"