package org.radargun.microbenchmark.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.MicrobenchmarkStressor;
import org.radargun.microbenchmark.domain.IntSet;

public class RemoveTransaction implements MicrobenchmarkTransaction {

    public final int node;
    public final int value;
    public final boolean local;
    public final boolean remote;
    public final boolean totalOrder;

    public RemoveTransaction(int node, int value, boolean local, boolean remote, boolean totalOrder) {
        this.node = node;
        this.value = value;
	this.local = local;
	this.remote = remote;
	this.totalOrder = totalOrder;
    }

    @Override
    public boolean executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
        LocatedKey key = cacheWrapper.createKey("SET" + node, node);
        IntSet intset = ((IntSet)cacheWrapper.get(null, key));
System.out.println("RemoveTransaction: " + key + " -- " + value + " remote? " + remote + " node- " + node);
        boolean res = intset.remove(cacheWrapper, this.value, local, remote);
//        if (totalOrder && remote) {
//            int otherNode = (node + 1) % MicrobenchmarkStressor.clients;
//            key = cacheWrapper.createKey("SET" + otherNode, otherNode);
//            intset = (IntSet)cacheWrapper.get(null, key);
//            intset.remove(cacheWrapper, this.value, local, remote);      
//        }
System.out.println("\t" + res);
        return res;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
    
}
