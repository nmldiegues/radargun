#!/bin/bash

WORKING_DIR=`cd $(dirname $0); cd ..; pwd`

DEST_FILE=${WORKING_DIR}/conf/jgroups/jgroups.xml

SEQUENCER="false"
TOA="false"
IP_MCAST="true"
INITIAL_HOST="localhost"

help() {
echo "usage: $0 <options>"
echo "  options:"
echo "    -sequencer             puts the SEQUENCER in protocol stack (Total Order Broadcast)"
echo ""
echo "    -toa                   puts the TOA in protocol stack (Total Order Anycast)"
echo ""
echo "    -no-ipmcast <address>  sets the protocol stack for the case where IP Multicast does not exists"
echo "                           The <address> is the address of the gossip router"
echo ""
echo "    -h                     show this message"
}

while [ -n "$1" ]; do
case $1 in
  -h) help; exit 0;;
  -sequencer) SEQUENCER="true"; shift 1;;
  -toa) TOA="true"; shift 1;;
  -no-ipmcast) IP_MCAST="false"; INITIAL_HOST=$2; shift 2;;
  -*) echo "WARNING: unknown option '$1'. It will be ignored" >&2; shift 1;;
  *) echo "WARNING: unknown argument '$1'. It will be ignored" >&2; shift 1;;
  esac
done

echo "Writing configuration to ${DEST_FILE}"

echo "<config" > ${DEST_FILE}
echo "      xmlns=\"urn:org:jgroups\"" >> ${DEST_FILE}
echo "      xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"" >> ${DEST_FILE}
echo "      xsi:schemaLocation=\"urn:org:jgroups http://www.jgroups.org/schema/JGroups-3.0.xsd\">" >> ${DEST_FILE}
echo "   <UDP" >> ${DEST_FILE}
echo "         tos=\"8\"" >> ${DEST_FILE}
echo "         ucast_recv_buf_size=\"20M\"" >> ${DEST_FILE}
echo "         ucast_send_buf_size=\"640K\"" >> ${DEST_FILE}

#only if we have IP Multicast
if [ "${IP_MCAST}" == "true" ]; then
echo "         mcast_recv_buf_size=\"25M\"" >> ${DEST_FILE}
echo "         mcast_send_buf_size=\"640K\"" >> ${DEST_FILE}
echo "         mcast_addr=\"\${jgroups.udp.mcast_addr:232.10.10.10}\"" >> ${DEST_FILE}
echo "         mcast_port=\"\${jgroups.udp.mcast_port:45588}\"" >> ${DEST_FILE}
fi

echo "         loopback=\"false\"" >> ${DEST_FILE}
echo "         discard_incompatible_packets=\"true\"" >> ${DEST_FILE}
echo "         max_bundle_size=\"64K\"" >> ${DEST_FILE}
echo "         max_bundle_timeout=\"2\"" >> ${DEST_FILE}
echo "         ip_ttl=\"\${jgroups.udp.ip_ttl:8}\"" >> ${DEST_FILE}
echo "         enable_bundling=\"true\"" >> ${DEST_FILE}
echo "         enable_unicast_bundling=\"true\"" >> ${DEST_FILE}
echo "         enable_diagnostics=\"true\"" >> ${DEST_FILE}
echo "         thread_naming_pattern=\"cl\"" >> ${DEST_FILE}
echo "         ip_mcast=\"${IP_MCAST}\"" >> ${DEST_FILE}

echo "         thread_pool.enabled=\"true\"" >> ${DEST_FILE}
echo "         thread_pool.min_threads=\"8\"" >> ${DEST_FILE}
echo "         thread_pool.max_threads=\"64\"" >> ${DEST_FILE}
echo "         thread_pool.keep_alive_time=\"30000\"" >> ${DEST_FILE}
echo "         thread_pool.queue_enabled=\"true\"" >> ${DEST_FILE}
echo "         thread_pool.queue_max_size=\"10000\"" >> ${DEST_FILE}
echo "         thread_pool.rejection_policy=\"discard\"" >> ${DEST_FILE}

echo "         oob_thread_pool.enabled=\"true\"" >> ${DEST_FILE}
echo "         oob_thread_pool.min_threads=\"8\"" >> ${DEST_FILE}
echo "         oob_thread_pool.max_threads=\"64\"" >> ${DEST_FILE}
echo "         oob_thread_pool.keep_alive_time=\"30000\"" >> ${DEST_FILE}
echo "         oob_thread_pool.queue_enabled=\"true\"" >> ${DEST_FILE}
echo "         oob_thread_pool.queue_max_size=\"10000\"" >> ${DEST_FILE}
echo "         oob_thread_pool.rejection_policy=\"discard\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

#only if we have IP Multicast
if [ "${IP_MCAST}" == "true" ]; then
echo "   <PING" >> ${DEST_FILE}
echo "         timeout=\"5000\"" >> ${DEST_FILE}
echo "         num_initial_members=\"10\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
else
echo "   <TCPGOSSIP" >> ${DEST_FILE}
echo "         initial_hosts=\"\${jgroups.gossip_host:${INITIAL__HOST}}[12001]\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
fi

echo "   <MERGE2" >> ${DEST_FILE}
echo "         max_interval=\"30000\"" >> ${DEST_FILE}
echo "         min_interval=\"10000\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <FD_SOCK/>" >> ${DEST_FILE}
echo "   <BARRIER/>" >> ${DEST_FILE}
echo "   <pbcast.NAKACK" >> ${DEST_FILE}
echo "         exponential_backoff=\"500\"" >> ${DEST_FILE}
echo "         use_mcast_xmit=\"${IP_MCAST}\"" >> ${DEST_FILE}
echo "         xmit_stagger_timeout=\"500\"" >> ${DEST_FILE}
echo "         discard_delivered_msgs=\"true\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <UNICAST" >> ${DEST_FILE}
echo "         xmit_interval=\"1000\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <pbcast.STABLE" >> ${DEST_FILE}
echo "         stability_delay=\"2000\"" >> ${DEST_FILE}
echo "         desired_avg_gossip=\"10000\"" >> ${DEST_FILE}
echo "         max_bytes=\"10M\"" >> ${DEST_FILE}
echo "         cap=\"0.1\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <pbcast.GMS" >> ${DEST_FILE}
echo "         print_local_addr=\"true\"" >> ${DEST_FILE}
echo "         join_timeout=\"3000\"" >> ${DEST_FILE}
echo "         max_bundling_time=\"500\"" >> ${DEST_FILE}
echo "         view_bundling=\"true\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}

#if sequencer
if [ "${SEQUENCER}" == "true" ]; then
echo "   <SEQUENCER/>" >> ${DEST_FILE}
fi

#if toa
if [ "${TOA}" == "true" ]; then
echo "   <tom.TOA/>" >> ${DEST_FILE}
fi

echo "   <UFC" >> ${DEST_FILE}
echo "         max_credits=\"4M\"" >> ${DEST_FILE}
echo "         min_threshold=\"0.4\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <MFC" >> ${DEST_FILE}
echo "         max_credits=\"4M\"" >> ${DEST_FILE}
echo "         min_threshold=\"0.4\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <FRAG2" >> ${DEST_FILE}
echo "         frag_size=\"60K\"" >> ${DEST_FILE}
echo "         />" >> ${DEST_FILE}
echo "   <pbcast.STATE_TRANSFER/>" >> ${DEST_FILE}
echo "</config>" >> ${DEST_FILE}

echo "Finished!"