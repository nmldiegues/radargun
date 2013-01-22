package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.domain.Manager;

public class DeleteCustomerOperation implements VacationTransaction {

    final private Manager managerPtr;
    final private int customerId;

    public DeleteCustomerOperation(Manager managerPtr, Random randomPtr, int queryRange) {
	this.managerPtr = managerPtr; 
	this.customerId = randomPtr.posrandom_generate() % queryRange + 1;
    }

    @Override
    public void executeTransaction(CacheWrapper cache) throws Throwable {
	int bill = managerPtr.manager_queryCustomerBill(cache, customerId);
	if (bill >= 0) {
	    managerPtr.manager_deleteCustomer(cache, customerId);
	}
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }
    
}
