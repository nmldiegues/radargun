package org.radargun.stamp.vacation;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.transaction.VacationTransaction;
import org.radargun.stressors.AbstractCacheWrapperStressor;

public class VacationStressor extends AbstractCacheWrapperStressor implements Runnable {

    private static Log log = LogFactory.getLog(VacationStressor.class);

    private CacheWrapper cacheWrapper;
    private VacationTransaction[] transactions;
    private int clients;
    private int threadid;
    private long restarts = 0;
    private long totalTime = 0;

    public static final ThreadLocal<Integer> THREADID = new ThreadLocal<Integer>() {};
    public static int CLIENTS;
    public static int MY_NODE;
    
    public void setCacheWrapper(CacheWrapper cacheWrapper) {
	this.cacheWrapper = cacheWrapper;
    }

    @Override
    public void run() {
	stress(cacheWrapper);
    }

    @Override
    public Map<String, String> stress(CacheWrapper wrapper) {
	THREADID.set(this.threadid);
	
	this.cacheWrapper = wrapper;

	long start = System.currentTimeMillis();
	for (int i = 0; i < transactions.length; i++) {
	    processTransaction(wrapper, transactions[i]);
	}
	this.totalTime = System.currentTimeMillis() - start;

	Map<String, String> results = new LinkedHashMap<String, String>();

	return results;
    }

    private void processTransaction(CacheWrapper wrapper, VacationTransaction transaction) {
	boolean successful = true;

	while (true) {
	    cacheWrapper.startTransaction(transaction.isReadOnly());
	    try {
		transaction.executeTransaction(cacheWrapper);
	    } catch (Throwable e) {
		successful = false;
	    }

	    try {
		cacheWrapper.endTransaction(successful);

		if (!successful) {
		    setRestarts(getRestarts() + 1);
		}
	    } catch (Throwable rb) {
		setRestarts(getRestarts() + 1);
		successful = false;
	    }
	    
	    if (! successful) {
		successful = true;
	    } else { 
		break;
	    }
	}
    }

    @Override
    public void destroy() throws Exception {
	cacheWrapper.empty();
	cacheWrapper = null;
    }

    public VacationTransaction[] getTransactions() {
	return transactions;
    }

    public void setTransactions(VacationTransaction[] transactions) {
	this.transactions = transactions;
    }
    
    public void setClients(int clients) {
	this.clients = clients;
    }

    public void setThreadid(int threadid) {
	this.threadid = threadid;
    }
    
    public long getRestarts() {
	return restarts;
    }

    public long getTotalTime() {
	return this.totalTime;
    }

    public void setRestarts(long restarts) {
	this.restarts = restarts;
    }

}
