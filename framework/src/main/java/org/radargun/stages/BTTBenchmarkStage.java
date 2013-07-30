package org.radargun.stages;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.btree.BTreeStressor;
import org.radargun.btt.BTTStressor;
import org.radargun.state.MasterState;

public class BTTBenchmarkStage extends AbstractDistStage {

    private static final String SIZE_INFO = "SIZE_INFO";

    private transient CacheWrapper cacheWrapper;
    
    private transient BTTStressor bttStressor;
    
    private int readOnlyPerc;
    private int keysSize;
    private int seconds;
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting BTTBenchmarkStage: " + this.toString());

	bttStressor = new BTTStressor();
	bttStressor.setCache(cacheWrapper);
	bttStressor.setReadOnlyPerc(readOnlyPerc);
	bttStressor.setKeysSize(keysSize);
	bttStressor.setSeconds(seconds);
	
	try {
	    Thread worker = new Thread(bttStressor);
	    worker.start();
	    try {
		Thread.sleep(seconds * 1000);
	    } catch (InterruptedException e) {
	    }
	    bttStressor.setM_phase(BTreeStressor.SHUTDOWN_PHASE);
	    worker.join();
	    
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    
	    long steps = bttStressor.steps;
	    long aborts = bttStressor.aborts;
	    long latency = bttStressor.totalLatency;
	    
	    results.put("TOTAL_THROUGHPUT", ((steps + 0.0) / (seconds + 0.0)) + "");
	    results.put("TOTAL_RESTARTS", aborts + "");
	    results.put("AVG_LATENCY", ((latency + 0.0) / (steps + 0.0)) + "");
	    log.info(sizeInfo);
	    result.setPayload(results);
	    return result;
	} catch (Exception e) {
	    log.warn("Exception while initializing the test", e);
	    result.setError(true);
	    result.setRemoteException(e);
	    return result;
	}
    }

    public boolean processAckOnMaster(List<DistStageAck> acks, MasterState masterState) {
	logDurationInfo(acks);
	boolean success = true;
	Map<Integer, Map<String, Object>> results = new HashMap<Integer, Map<String, Object>>();
	masterState.put("results", results);
	for (DistStageAck ack : acks) {
	    DefaultDistStageAck wAck = (DefaultDistStageAck) ack;
	    if (wAck.isError()) {
		success = false;
		log.warn("Received error ack: " + wAck);
	    } else {
		if (log.isTraceEnabled())
		    log.trace(wAck);
	    }
	    Map<String, Object> benchResult = (Map<String, Object>) wAck.getPayload();
	    if (benchResult != null) {
		results.put(ack.getSlaveIndex(), benchResult);
		Object reqPerSes = benchResult.get("TOTAL_THROUGHPUT");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there! TOTAL_THROUGHPUT");
		}
		Object aborts = benchResult.get("TOTAL_RESTARTS");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there! TOTAL_RESTARTS");
		}
		Object latency = benchResult.get("AVG_LATENCY");
		if (reqPerSes == null) {
		    throw new IllegalStateException("This should be there! AVG_LATENCY");
		}
		log.info("On slave " + ack.getSlaveIndex() + " had throughput " + Double.parseDouble(reqPerSes.toString()) + " ops/seconds | aborts: " + Long.parseLong(aborts.toString()) + " | latency: " + Double.parseDouble(latency.toString()));
		log.info("Received " +  benchResult.remove(SIZE_INFO));
	    } else {
		log.trace("No report received from slave: " + ack.getSlaveIndex());
	    }
	}
	return success;
    }
    
    public void setReadOnlyPerc(int readOnlyPerc){
	this.readOnlyPerc = readOnlyPerc;
    }
    public int getReadOnlyPerc() {
	return this.readOnlyPerc;
    }
    public int getKeysSize() {
        return keysSize;
    }
    public void setKeysSize(int keysSize) {
        this.keysSize = keysSize;
    }
    public int getSeconds() {
        return seconds;
    }
    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }
}
