package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;

public class AddTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;
    public final boolean local;
    public final boolean remote;

    public AddTransaction(int node, int value, boolean local, boolean remote) {
        this.node = node;
        this.value = value;
        this.local = local;
	this.remote = remote;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
        IntSet set = (IntSet)cacheWrapper.get(null, key);
        return set.add(cacheWrapper, this.value, local, remote);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

}
