package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;

public class RemoveTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;

    public RemoveTransaction(int node, int value) {
        this.node = node;
        this.value = value;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
        return ((IntSet)cacheWrapper.get(null, key)).remove(cacheWrapper, this.value);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
    
}
