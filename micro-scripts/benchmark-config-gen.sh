#!/bin/bash

#default values
if [ -n "${BENCH_XML_FILEPATH}" ]; then
  DEST_FILE=${BENCH_XML_FILEPATH}
else
  DEST_FILE=./benchmark.xml
fi
CLIENTS=1
WRITE_RATIO=10
DURATION=10000
LOCAL_THREADS=1
ITEMS=64
RANGE=4096
SET=ll

if [ -n "${ISPN_CONFIG_FILENAME}" ]; then
  CONFIGURATION_FILE=${ISPN_CONFIG_FILENAME}
else
  CONFIGURATION_FILE="config.xml" 
fi

help_and_exit(){
echo "usage: ${0} <options>"
echo "options:"
echo "  -c <value>               number of clients (also means number of nodes used)"
echo "                           default: ${CLIENTS}"
echo ""
echo "  -l <value>               number of threads per node"
echo "                           default: ${LOCAL_THREADS}"
echo ""
echo "  -d <value>               duration of the benchmark (ms)"
echo "                           default: ${DURATION}"
echo ""
echo "  -w <value>               the ratio of write transactions"
echo "                           default: ${WRITE_RATIO}"
echo ""
echo "  -i <value>               the number of items in the collection"
echo "                           default: ${ITEMS}"
echo ""
echo "  -r <value>        	 the range of possible values for the items"
echo "                           default: ${RANGE}"
echo ""
echo "  -s <value>               the set to be used"
echo "                           default: ${SET}"
echo ""
echo ""
echo "  -h                       show this message and exit"
exit 0
}

while [ -n $1 ]; do
case $1 in
  -h) help_and_exit;;
  -c) CLIENTS=$2; shift 2;;
  -l) LOCAL_THREADS=$2; shift 2;;
  -w) WRITE_RATIO=$2; shift 2;;
  -d) DURATION=$2; shift 2;;
  -i) ITEMS=$2; shift 2;;
  -r) RANGE=$2; shift 2;;
  -s) SET=$2; shift 2;;
  -*) echo "unknown option $1"; exit 1;;
  *) break;;
esac
done

echo "Writing configuration to ${DEST_FILE}"

echo "<bench-config>" > ${DEST_FILE}

echo "   <master" >> ${DEST_FILE}
echo "         bindAddress=\"\${127.0.0.1:master.address}\"" >> ${DEST_FILE}
echo "         port=\"\${21032:master.port}\"/>" >> ${DEST_FILE}

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

echo "      <MicrobenchmarkPopulation" >> ${DEST_FILE}
echo "            relations=\"${RELATIONS}\" />" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_BEFORE_BENCH\" />" >> ${DEST_FILE}

echo "      <Microbenchmark" >> ${DEST_FILE}
echo "            clients=\"${CLIENTS}\"" >> ${DEST_FILE}
echo "            localThreads=\"${LOCAL_THREADS}\"" >> ${DEST_FILE}
echo "            duration=\"${DURATION}\"" >> ${DEST_FILE}
echo "            writeRatio=\"${WRITE_RATIO}\"" >> ${DEST_FILE}
echo "            items=\"${ITEMS}\"" >> ${DEST_FILE}
echo "            range=\"${RANGE}\"" >> ${DEST_FILE}
echo "            set=\"${SET}\"/>" >> ${DEST_FILE}

echo "      <CacheSize" >> ${DEST_FILE}
echo "            statName=\"CACHE_SIZE_AFTER_BENCH\" />" >> ${DEST_FILE}

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
