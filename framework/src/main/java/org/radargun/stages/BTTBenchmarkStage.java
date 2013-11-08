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
    
    private transient BTTStressor[] bttStressors;
    
    private int readOnlyPerc;
    private int keysRange;
    private int keysSize;
    private int seconds;
    private String emulation;
    private int threads;
    
    public void setThreads(int threads) {
	this.threads = threads;
    }

    public void setEmulation(String emulation) {
        this.emulation = emulation;
    }
    
    public void setKeysRange(int keysRange) {
        this.keysRange = keysRange;
    }

    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck result = new DefaultDistStageAck(slaveIndex, slaveState.getLocalAddress());
	this.cacheWrapper = slaveState.getCacheWrapper();
	if (cacheWrapper == null) {
	    log.info("Not running test on this slave as the wrapper hasn't been configured.");
	    return result;
	}

	log.info("Starting BTTBenchmarkStage: " + this.toString());

	
	try {
	    Thread[] workers = new Thread[threads];
	    bttStressors = new BTTStressor[threads];
	    
	    for (int i = 0; i < threads; i++) {
		bttStressors[i] = new BTTStressor();
		bttStressors[i].setCache(cacheWrapper);
		bttStressors[i].setReadOnlyPerc(readOnlyPerc);
		bttStressors[i].setKeysSize(keysSize);
		bttStressors[i].setKeysRange(keysRange);
		bttStressors[i].setSeconds(seconds);
		bttStressors[i].setEmulation(emulation);
		workers[i] = new Thread(bttStressors[i]); 
	    }
	    
	    for (int i = 0; i < threads; i++) {
		workers[i].start();
	    }
	    try {
		Thread.sleep(seconds * 1000);
	    } catch (InterruptedException e) {
	    }
	    for (int i = 0; i < threads; i++) {
		bttStressors[i].setM_phase(BTreeStressor.SHUTDOWN_PHASE);
	    }
	    for (int i = 0; i < threads; i++) {
		workers[i].join();
	    }
	    
	    Map<String, String> results = new LinkedHashMap<String, String>();
	    String sizeInfo = "size info: " + cacheWrapper.getInfo() +
		    ", clusterSize:" + super.getActiveSlaveCount() +
		    ", nodeIndex:" + super.getSlaveIndex() +
		    ", cacheSize: " + cacheWrapper.getCacheSize();
	    results.put(SIZE_INFO, sizeInfo);
	    
	    long steps = 0; 
	    long aborts = 0; 
	    
	    for (int i = 0; i < threads; i++) {
		steps += bttStressors[i].steps;
		aborts += bttStressors[i].aborts;
	    }
	    
	    results.put("TOTAL_THROUGHPUT", ((steps + 0.0) / (seconds + 0.0)) + "");
	    results.put("TOTAL_RESTARTS", aborts + "");
	    results.putAll(this.cacheWrapper.getAdditionalStats());
	    
	    String str = "";
	    for (int i = 0; i < threads; i++) {
		Map<Integer, Long> latencies = bttStressors[i].latencies;
		for (Map.Entry<Integer, Long> entry : latencies.entrySet()) {
		    int latency = entry.getKey();
		    double val = entry.getValue();
		    str += latency + ":" + val + ";";
		}
	    }
	    results.put("LATENCY", str);
	    
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
