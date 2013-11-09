package org.radargun.btt;

import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.btt.colocated.BPlusTree;
import org.radargun.btt.generators.CoreWorkload;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class BTTPopulationStressor extends AbstractCacheWrapperStressor{

    private static Log log = LogFactory.getLog(BTTPopulationStressor.class);

    private int keysSize;
    private int keysRange;
    private String workload;
    private boolean threadMigration;
    private boolean ghostReads;
    private boolean colocation;
    private boolean replicationDegrees;
    private boolean intraNodeConc;
    private int lowerBound;

    public void setWorkload(String workload) {
	this.workload = workload;
    }
    
    public void setIntraNodeConc(boolean intraNodeConc) {
	this.intraNodeConc = intraNodeConc;
    }
    
    public void setKeysRange(int keysRange) {
        this.keysRange = keysRange;
    }

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
System.exit(1);
	}
	return null;
    }

    public Properties generateProperties(String workload) {
	Properties p = new Properties();
	if (workload.equals("a")) {
	    p.setProperty(CoreWorkload.RECORD_COUNT_PROPERTY, keysSize + "");
	    p.setProperty(CoreWorkload.OPERATION_COUNT_PROPERTY, "1000");
	    p.setProperty(CoreWorkload.WORKLOAD_PROPERTY, "org.radargun.btt.generators.CoreWorkload");
	    p.setProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, "true");
	    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0.5");
	    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0.5");
	    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, "zipfian");
	} else if (workload.equals("b")) {
	    p.setProperty(CoreWorkload.RECORD_COUNT_PROPERTY, keysSize + "");
	    p.setProperty(CoreWorkload.OPERATION_COUNT_PROPERTY, "1000");
	    p.setProperty(CoreWorkload.WORKLOAD_PROPERTY, "org.radargun.btt.generators.CoreWorkload");
	    p.setProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, "true");
	    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0.95");
	    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0.05");
	    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, "zipfian");
	} else if (workload.equals("c")) {
	    p.setProperty(CoreWorkload.RECORD_COUNT_PROPERTY, keysSize + "");
	    p.setProperty(CoreWorkload.OPERATION_COUNT_PROPERTY, "1000");
	    p.setProperty(CoreWorkload.WORKLOAD_PROPERTY, "org.radargun.btt.generators.CoreWorkload");
	    p.setProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, "true");
	    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "1");
	    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, "zipfian");
	} else if (workload.equals("d")) {
	    p.setProperty(CoreWorkload.RECORD_COUNT_PROPERTY, keysSize + "");
	    p.setProperty(CoreWorkload.OPERATION_COUNT_PROPERTY, "1000");
	    p.setProperty(CoreWorkload.WORKLOAD_PROPERTY, "org.radargun.btt.generators.CoreWorkload");
	    p.setProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, "true");
	    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0.95");
	    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0.05");
	    p.setProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, "latest");
	} else if (workload.equals("e")) {
	    p.setProperty(CoreWorkload.RECORD_COUNT_PROPERTY, keysSize + "");
	    p.setProperty(CoreWorkload.OPERATION_COUNT_PROPERTY, "1000");
	    p.setProperty(CoreWorkload.WORKLOAD_PROPERTY, "org.radargun.btt.generators.CoreWorkload");
	    p.setProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, "true");
	    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0.95");
	    p.setProperty(CoreWorkload.SCAN_LENGTH_DISTRIBUTION_PROPERTY, "uniform");
	    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0.05");
	    p.setProperty(CoreWorkload.MAX_SCAN_LENGTH_PROPERTY, "100");
	    p.setProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, "zipfian");
	} else if (workload.equals("f")) {
	    p.setProperty(CoreWorkload.RECORD_COUNT_PROPERTY, keysSize + "");
	    p.setProperty(CoreWorkload.OPERATION_COUNT_PROPERTY, "1000");
	    p.setProperty(CoreWorkload.WORKLOAD_PROPERTY, "org.radargun.btt.generators.CoreWorkload");
	    p.setProperty(CoreWorkload.READ_ALL_FIELDS_PROPERTY, "true");
	    p.setProperty(CoreWorkload.READ_PROPORTION_PROPERTY, "0.5");
	    p.setProperty(CoreWorkload.UPDATE_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.INSERT_PROPORTION_PROPERTY, "0");
	    p.setProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY, "0.5");
	    p.setProperty(CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY, "zipfian");
	}
	
	return p;
    }
    
    public static transient CoreWorkload w;
    
    private void populate(CacheWrapper wrapper) {
	int clusterSize = wrapper.getNumMembers();
	wrapper.initDEF();
	BPlusTree.wrapper = wrapper;
	BPlusTree.LOWER_BOUND = lowerBound;
	BPlusTree.LOWER_BOUND_WITH_LAST_KEY = BPlusTree.LOWER_BOUND + 1;
	// The maximum number of keys in a node NOT COUNTING with the special LAST_KEY. This number should be a multiple of 2.
	BPlusTree.MAX_NUMBER_OF_KEYS = 2 * BPlusTree.LOWER_BOUND;
	BPlusTree.MAX_NUMBER_OF_ELEMENTS = BPlusTree.MAX_NUMBER_OF_KEYS + 1;
	
	BPlusTree.MEMBERS = clusterSize;
	BPlusTree.THREAD_MIGRATION = threadMigration;
	BPlusTree.COLOCATE = colocation;
	BPlusTree.GHOST = ghostReads;
	BPlusTree.REPL_DEGREES = replicationDegrees;
	BPlusTree.INTRA_NODE_CONC = intraNodeConc;
	BPlusTree.POPULATING = true;
	
	if (!workload.equals("X")) {
	    Properties p = generateProperties(workload);
	    w = new CoreWorkload();
	    w.init(p);
	}
	
	if (wrapper.isTheMaster()) {
	    LocatedKey treeKey = wrapper.createGroupingKeyWithRepl("tree", 0, clusterSize);
	    BPlusTree<Long> tree = new BPlusTree<Long>(clusterSize, threadMigration, colocation, ghostReads, replicationDegrees);
	    try {
		wrapper.put(treeKey, tree);
	    } catch (Exception e) {
		e.printStackTrace();
		System.exit(-1);
	    }

//	    int batch = 1000;
//	    for (int i = 0; i < this.keysSize; i += batch) {
//		doPopulation(wrapper, tree, i, batch);
//		System.out.println("Coordinator inserted: " + i + " // " + this.keysSize);
//	    }
	    
	    int threads = Runtime.getRuntime().availableProcessors();
	    
	    long[] arr = new long[this.keysRange];
	    for (int i = 0; i < this.keysRange; i ++) {
		arr[i] = w.getIntForInsert();
	    }
	    
	    Worker[] workers = new Worker[threads];
	    int division = this.keysRange / threads;
	    int batch = 0;
	    for (int i = 0; i < workers.length; i++) {
		workers[i] = new Worker(tree, batch, batch + division, arr, wrapper);
		System.out.println("Setup worker " + i + "th with min " + batch + " and max " + (batch + division));
		batch += division;
	    }
	    
	    workers[0].start();
	    
	    try {
		Thread.sleep(20000);
	    } catch (InterruptedException e1) {
	    }
	    
	    for (int i = 1; i < threads; i++) {
		workers[i].start();
	    }
	    for (int i = 0; i < threads; i++) {
		try {
		    workers[i].join();
		} catch (InterruptedException e) {
		}
	    }
	    
	    System.out.println("Starting colocation!");
	    while (tree.colocate(treeKey)) {System.out.println("Successful colocation!");}
	    System.out.println("Finished colocation!");
	    try { Thread.sleep(2000); } catch (Exception e) {}

	    Map<String, String> stats = wrapper.getAdditionalStats();
	    System.out.println(stats);

	    wrapper.resetAdditionalStats();
	    
	    wrapper.put(wrapper.createGroupingKeyWithRepl("minuetFull-snapshot-id", 0, clusterSize), 0L);
	    wrapper.put(wrapper.createGroupingKey("minuetFull-snapshot-id", 0), 0L);
	}
	BPlusTree.POPULATING = false;
    }
    
    private void doPopulation(CacheWrapper wrapper, BPlusTree<Long> tree, int start, int batch) {
	boolean successful = false;
	while (!successful) {
	    try {
		wrapper.startTransaction(false);
		
		for (int i = start; i < (start + batch); i++) {
		    long nextVal = i;
		    if (w != null) {
			nextVal = w.getIntForInsert();
		    }
		    if (tree.insert(nextVal)) {
			wrapper.endTransaction(true);
			System.out.println("\tinserted: " + i + " of range " + start + " <-> " + batch);
			wrapper.startTransaction(false);
		    }
		}
		
		wrapper.endTransaction(true);
		successful = true;
	    } catch (Exception e) {
		e.printStackTrace();
		try { wrapper.endTransaction(false); 
		} catch (Exception e2) { }
throw new RuntimeException(e);
	    }
	}
    }
    
    public class Worker extends Thread {
	
	public final int min;
	public final int max;
	public final long[] arr;
	public final CacheWrapper wrapper;
	public final BPlusTree<Long> tree;
	public final Random ran;
	private long timeout;
	
	public Worker(BPlusTree<Long> tree, int min, int max, long[] arr, CacheWrapper wrapper) {
	    this.tree = tree;
	    this.min = min;
	    this.max = max;
	    this.arr = arr;
	    this.wrapper = wrapper;
	    this.ran = new Random();
	}
	
	@Override
	public void run() {
	    int batch = 10;
	    for (int i = min; i < max; i += batch) {
		populate(i, batch);
		double dmin = (double) min;
		double dmax = (double) max;
		double di = (double) i;
		int perc = (int)((di - dmin) / (dmax - dmin) * 100.0);
		if (i % 50000 == 0) {
		    System.out.println(Thread.currentThread().getId() + "] Coordinator inserted: " + i + " " + max + " -- " + perc + " %");
		}
		timeout = 10;
	    }
	}
	
	private void populate(int start, int batch) {
	    boolean successful = false;
	    while (!successful) {
		try {
		    wrapper.startTransaction(false);

		    for (int i = start; i < (start + batch); i++) {
			long nextVal = arr[i];
			if (tree.insert(nextVal)) {
			    wrapper.endTransaction(true);
			    wrapper.startTransaction(false);
			}
		    }

		    wrapper.endTransaction(true);
		    successful = true;
		} catch (Exception e) {
		    try { wrapper.endTransaction(false); 
		    } catch (Exception e2) { }
//		    try {
//			Thread.sleep(timeout);
//		    } catch (InterruptedException e1) {
//		    }
//		    
//		    timeout = timeout + this.ran.nextInt(100);
		}
	    }
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
