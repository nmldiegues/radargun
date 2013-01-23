package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.microbenchmark.domain.IntSet;

public class ContainsTransaction implements MicrobenchmarkTransaction {

    public final IntSet set;
    public final int value;
    
    public ContainsTransaction(IntSet set, int value) {
	this.set = set;
	this.value = value;
    }
    
    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	return this.set.contains(cacheWrapper, this.value);
    }

    @Override
    public boolean isReadOnly() {
	return true;
    }
}
