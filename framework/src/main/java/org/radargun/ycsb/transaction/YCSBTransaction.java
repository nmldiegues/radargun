package org.radargun.ycsb.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;

public abstract class YCSBTransaction {

    protected boolean remote;
    protected int node;
    
    public YCSBTransaction(int random, int remotePerc) {
	this.remote = (random % 100) < remotePerc;
	if (this.remote) {
	    this.node = random % VacationStressor.CLIENTS;
	} else {
	    this.node = VacationStressor.MY_NODE;
	}
    }
    
    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    public abstract boolean isReadOnly();
}
    
