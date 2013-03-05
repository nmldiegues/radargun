package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.VacationStressor;

public abstract class VacationTransaction {

    protected boolean remote;
    protected int node;

    public VacationTransaction(int random) {
	this.remote = (random % 100) < 20;
	if (this.remote) {
	    this.node = random % VacationStressor.CLIENTS;
	} else {
	    this.node = VacationStressor.MY_NODE;
	}
    }
    
    public abstract void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    public abstract boolean isReadOnly();
}
