package org.radargun.btree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.radargun.CacheWrapper;
import org.radargun.DEF;
import org.radargun.DEFTask;
import org.radargun.LocatedKey;
import org.radargun.stressors.AbstractCacheWrapperStressor;


public class BTreeStressor extends AbstractCacheWrapperStressor implements Runnable {

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;
    
    public static CacheWrapper cache;
    private String execMode;
    private int remoteProb;
    private int opPerTx;
    private int keysSize;
    private int seconds;
    
    private Random random = new Random();
    
    public int steps;
    
    volatile protected int m_phase = TEST_PHASE;
    
    public DEF des;
    
    // groups.length = clusterSize - 1; one int/group per member of the cluster != local
    private int[] groups;
    private int myGroup;
    
    @Override
    public void run() {
	stress(cache);
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }
        
        des = cache.createDEF();
        
        try {
            cache.startTransaction(true);
            Object myAddress = cache.getMyAddress();
            List others = new ArrayList(cache.getAllAddresses());
            others.remove(myAddress);
            myGroup = Integer.parseInt((String) cache.get(null, myAddress + "-id"));
            int i = 0;
            groups = new int[others.size()];
            for (Object member : others) {
        	groups[i] = Integer.parseInt((String) cache.get(null, member + "-id"));
System.err.println("Group " + member + " " + groups[i]);
		i++;
            }
System.err.println("Exec mode: " + execMode + " remote prob " + remoteProb + " opPerTx " + opPerTx + " keysSize " + keysSize + " seconds " + seconds + " myGroup " + myGroup);
            cache.endTransaction(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Should not have had an exception here..!");
            System.exit(1);
        }
        
        while (m_phase == TEST_PHASE) {
            step(TEST_PHASE);
            steps++;
        }
        
        Map<String, String> results = new LinkedHashMap<String, String>();

        return results;
    }
    
    protected void step(int phase) {
	try {

	    boolean remote = (Math.abs(random.nextInt()) % 100) < remoteProb;
	    int k = Math.abs(random.nextInt()) % keysSize;
	    int id = !remote ? myGroup : groups[Math.abs(random.nextInt()) % groups.length];

	    DEFTask<Boolean> task = cache.createTask(new DistCallable(k, id, keysSize, opPerTx));
	    boolean success = false;
	    if (execMode.equals("DEF_LOCAL")) {
		if (!remote) {
		    success = task.justExecute();
		} else {
		    Future<Boolean> answer = des.submit(task, cache.createGroupingKey("key" + k, id));
		    success = answer.get();
		}
	    } else if (execMode.equals("DEF")){
		Future<Boolean> answer = des.submit(task, cache.createGroupingKey("key" + k, id));
		success = answer.get();
	    } else {
		success = task.justExecute();
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	    System.err.println("Should not have had an exception here..!");
	    System.exit(1);
	}
    }
    
    @Override
    public void destroy() throws Exception {
        cache.empty();
        cache = null;
    }
    
    public CacheWrapper getCache() {
        return cache;
    }
    public void setCache(CacheWrapper cacheNew) {
        cache = cacheNew;
    }
    public String getExecMode() {
        return execMode;
    }
    public void setExecMode(String execMode) {
        this.execMode = execMode;
    }
    public int getRemoteProb() {
        return remoteProb;
    }
    public void setRemoteProb(int remoteProb) {
        this.remoteProb = remoteProb;
    }
    public int getOpPerTx() {
        return opPerTx;
    }
    public void setOpPerTx(int opPerTx) {
        this.opPerTx = opPerTx;
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
    public int getM_phase() {
        return m_phase;
    }
    public void setM_phase(int m_phase) {
        this.m_phase = m_phase;
    }
}
