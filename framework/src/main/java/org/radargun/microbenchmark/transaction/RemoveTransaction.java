package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;

public class RemoveTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;
    public final boolean local;
    public final boolean remote;

    public RemoveTransaction(int node, int value, boolean local, boolean remote) {
        this.node = node;
        this.value = value;
	this.local = local;
	this.remote = remote;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
        IntSet intset = ((IntSet)cacheWrapper.get(null, key));
        return intset.remove(cacheWrapper, this.value, local, remote);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
    
}
