package org.radargun.cachewrappers;

import java.io.Serializable;
import java.util.concurrent.Callable;

import org.infinispan.transaction.CacheCallable;
import org.radargun.CallableWrapper;

public class CacheCallableWrapper<T> extends CacheCallable<T> implements CallableWrapper<T>, Serializable {

    private Callable<T> task;
    
    public CacheCallableWrapper(Callable<T> task) {
	this.task = task;
    }
    
    @Override
    public T call() throws Exception {
	return this.task.call();
    }

    @Override
    public T doTask() throws Exception {
	return call();
    }

}
