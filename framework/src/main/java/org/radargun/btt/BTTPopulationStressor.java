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
    private int lowerBound;

    public void setLowerBound(int lowerBound) {
	this.lowerBound = lowerBound;
    }

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
	BPlusTree.LOWER_BOUND = lowerBound;
	BPlusTree.LOWER_BOUND_WITH_LAST_KEY = BPlusTree.LOWER_BOUND + 1;
	// The maximum number of keys in a node NOT COUNTING with the special LAST_KEY. This number should be a multiple of 2.
	BPlusTree.MAX_NUMBER_OF_KEYS = 2 * BPlusTree.LOWER_BOUND;
	BPlusTree.MAX_NUMBER_OF_ELEMENTS = BPlusTree.MAX_NUMBER_OF_KEYS + 1;
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
	    boolean successful = false;
	    while (!successful) {
		try {
		    wrapper.startTransaction(false);

		    for (int i = 0; i < this.keysSize; i++) {
			boolean duplicate = true;
			while (duplicate) {
			    long nextValue = Math.abs(r.nextLong());
			    if (tree.insert(nextValue, nextValue)) {
				duplicate = false;
			    }
			}
			if (i % 100 == 0) {
			    System.out.println("Coordinator inserted: " + i + " / " + this.keysSize);
			}
		    }

		    wrapper.endTransaction(true);
		    successful = true;
		} catch (Exception e) {
		    e.printStackTrace();
		    try { wrapper.endTransaction(false); 
		    } catch (Exception e2) { }
		}
	    }
	    System.out.println("Starting colocation!");
	    while (tree.colocate()) {System.out.println("Successful colocation!");}
	    System.out.println("Finished colocation!");
	    try { Thread.sleep(2000); } catch (Exception e) {}

	    Map<String, String> stats = wrapper.getAdditionalStats();
	    System.out.println(stats);

	    wrapper.resetAdditionalStats();
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
