package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;

public class AddTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;
    public final boolean local;

    public AddTransaction(int node, int value, boolean local) {
        this.node = node;
        this.value = value;
        this.local = local;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
        IntSet set = (IntSet)cacheWrapper.get(null, key);
        return set.add(cacheWrapper, this.value, local);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

}
