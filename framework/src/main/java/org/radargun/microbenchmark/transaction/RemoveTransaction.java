package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.microbenchmark.domain.IntSet;

public class RemoveTransaction implements MicrobenchmarkTransaction {

    public final IntSet set;
    public final int value;
    
    public RemoveTransaction(IntSet set, int value) {
	this.set = set;
	this.value = value;
    }
    
    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	return this.set.remove(cacheWrapper, this.value);
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }

    
}
