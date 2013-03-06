package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.Vacation;
import org.radargun.stamp.vacation.domain.Manager;

public class DeleteCustomerOperation extends VacationTransaction {

    final private int customerId;

    public DeleteCustomerOperation(Random randomPtr, int queryRange) {
	super(randomPtr.random_generate(), queryRange);
	this.customerId = randomPtr.posrandom_generate() % queryRange + 1;
    }

    @Override
    public void executeTransaction(CacheWrapper cache) throws Throwable {
	Vacation.NODE_TARGET.set(super.node);
	LocatedKey key = cache.createKey("MANAGER" + super.node, super.node);
	Manager managerPtr = (Manager) cache.get(null, key);
	int bill = managerPtr.manager_queryCustomerBill(cache, customerId);
	if (bill >= 0) {
	    if (!remote) {
	    managerPtr.manager_deleteCustomer(cache, customerId);
	    } else {
		managerPtr.manager_doCustomer(cache);
	    }
	}
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }
    
}
