package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;

public interface VacationTransaction {

    void executeTransaction(CacheWrapper cacheWrapper) throws Throwable;
    
    boolean isReadOnly();
}
