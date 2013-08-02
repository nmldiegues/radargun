package org.radargun.btt;

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
import org.radargun.btt.colocated.BPlusTree;
import org.radargun.stressors.AbstractCacheWrapperStressor;


public class BTTStressor extends AbstractCacheWrapperStressor implements Runnable {

    public static final int TEST_PHASE = 2;
    public static final int SHUTDOWN_PHASE = 3;
    
    public static CacheWrapper cache;
    private int readOnlyPerc;
    private int keysSize;
    private int keysRange;
    private int seconds;
    
    public void setKeysRange(int keysRange) {
        this.keysRange = keysRange;
    }

    private Random random = new Random();
    
    private BPlusTree<Long> tree;
    public long lastValue = -1L;
    public long steps;
    public long totalLatency;
    public long aborts;
    
    volatile protected int m_phase = TEST_PHASE;
    
    @Override
    public void run() {
	stress(cache);
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }

        int clusterSize = wrapper.getNumMembers();
        LocatedKey treeKey = wrapper.createGroupingKeyWithRepl("tree", 0, clusterSize);
        try {
            wrapper.startTransaction(true);
            this.tree = (BPlusTree<Long>) wrapper.get(treeKey);
            wrapper.endTransaction(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        while (m_phase == TEST_PHASE) {
            long start = System.nanoTime();
            step(TEST_PHASE);
            totalLatency += System.nanoTime() - start;
            steps++;
        }
        
        return new LinkedHashMap<String, String>();
    }
    
    protected void step(int phase) {
	boolean successful = false;
	long value = Math.abs(random.nextLong()) % keysRange;
	boolean query = Math.abs(random.nextInt(100)) < this.readOnlyPerc;
	while (!successful && m_phase == TEST_PHASE) {
	    try {
		cache.startTransaction(false);
		
		if (query) {
		    this.tree.containsKey(value);
		} else {
		    if (lastValue == -1) {
			this.tree.insert(value, value);
		    } else {
			this.tree.removeKey(lastValue);
		    }
		}
		
		cache.endTransaction(true);
		successful = true;
		if (!query) {
		    if (lastValue == -1) {
			lastValue = value;
		    } else {
			lastValue = -1;
		    }
		}
	    } catch (Exception e) {
		this.aborts++;
		try {
		    cache.endTransaction(false);
		} catch (Exception e2) {}
	    }
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
    public int getM_phase() {
        return m_phase;
    }
    public void setM_phase(int m_phase) {
        this.m_phase = m_phase;
    }
}
