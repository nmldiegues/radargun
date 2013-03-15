package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.domain.Manager;

public class DeleteCustomerOperation extends VacationTransaction {

    final private int customerId;

    public DeleteCustomerOperation(Random randomPtr, int queryRange, int relations) {
	this.customerId = randomPtr.posrandom_generate() % relations;
    }

    @Override
    public void executeTransaction(CacheWrapper cache) throws Throwable {
	Manager managerPtr = (Manager) cache.get(null, "MANAGER");
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
