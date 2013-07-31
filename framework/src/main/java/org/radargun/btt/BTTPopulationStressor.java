package org.radargun.btt;

import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.btt.colocated.BPlusTree;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class BTTPopulationStressor extends AbstractCacheWrapperStressor{

    private static Log log = LogFactory.getLog(BTTPopulationStressor.class);
    
    private int keysSize;
    private boolean threadMigration;
    private boolean ghostReads;
    private boolean colocation;
    private boolean replicationDegrees;
    
    public void setKeysSize(int keysSize) {
	this.keysSize = keysSize;
    }
    
    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
        if (wrapper == null) {
            throw new IllegalStateException("Null wrapper not allowed");
        }
        try {
            log.info("Performing Population Operations");
            populate(wrapper);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("Received exception during cache population" + e.getMessage());
        }
        return null;
    }
 
    private void populate(CacheWrapper wrapper) {
	int clusterSize = wrapper.getNumMembers();
	wrapper.initDEF();
	BPlusTree.wrapper = wrapper;
	if (wrapper.isTheMaster()) {
	    LocatedKey treeKey = wrapper.createGroupingKeyWithRepl("tree", 0, clusterSize);
	    BPlusTree<Long> tree = new BPlusTree<Long>(clusterSize, threadMigration, colocation, ghostReads, replicationDegrees);
	    try {
		wrapper.put(treeKey, tree);
	    } catch (Exception e) {
		e.printStackTrace();
		System.exit(-1);
	    }
	    Random r = new Random();
	    for (int i = 0; i < this.keysSize; i++) {
		boolean successful = false;
		while (!successful) {
		    try {
			wrapper.startTransaction(false);
			
			boolean duplicate = true;
			while (duplicate) {
			    long nextValue = Math.abs(r.nextLong());
			    if (tree.insert(nextValue, nextValue)) {
				duplicate = false;
			    }
			}
			
			wrapper.endTransaction(true);
			successful = true;
			if (i % 100 == 0) {
			    System.out.println("Coordinator inserted: " + i + " / " + this.keysSize);
			}
		    } catch (Exception e) {
			e.printStackTrace();
			try { wrapper.endTransaction(false); 
			} catch (Exception e2) { }
		    }
		}
	    }
	    while (tree.colocate()) {}
	    try { Thread.sleep(2000); } catch (Exception e) {}
	}
    }

    @Override
    public void destroy() throws Exception {
        //Don't destroy data in cache!
    }

    public boolean isThreadMigration() {
        return threadMigration;
    }

    public void setThreadMigration(boolean threadMigration) {
        this.threadMigration = threadMigration;
    }

    public boolean isGhostReads() {
        return ghostReads;
    }

    public void setGhostReads(boolean ghostReads) {
        this.ghostReads = ghostReads;
    }

    public boolean isColocation() {
        return colocation;
    }

    public void setColocation(boolean colocation) {
        this.colocation = colocation;
    }

    public boolean isReplicationDegrees() {
        return replicationDegrees;
    }

    public void setReplicationDegrees(boolean replicationDegrees) {
        this.replicationDegrees = replicationDegrees;
    }

    
}
