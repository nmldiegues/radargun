package org.radargun.stamp.vacation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.domain.Manager;

public class VacationPopulation {

    private static Log log = LogFactory.getLog(VacationPopulation.class);
    
    private final CacheWrapper wrapper;
    private int relations;
    
    public VacationPopulation(CacheWrapper wrapper, int relations) {
	this.wrapper = wrapper;
	this.relations = relations;
    }

    public void performPopulation(){

	int i;
	int t;

	Random randomPtr = new Random();
	randomPtr.random_alloc();
	Manager managerPtr = new Manager(wrapper, true);

	int numRelation = relations;
	int ids[] = new int[numRelation];
	for (i = 0; i < numRelation; i++) {
	    ids[i] = i + 1;
	}

	for (t = 0; t < 4; t++) {

	    /* Shuffle ids */
	    for (i = 0; i < numRelation; i++) {
		int x = randomPtr.posrandom_generate() % numRelation;
		int y = randomPtr.posrandom_generate() % numRelation;
		int tmp = ids[x];
		ids[x] = ids[y];
		ids[y] = tmp;
	    }

	    /* Populate table */
	    for (i = 0; i < numRelation; i++) {
		boolean status = false;
		int id = ids[i];
		int num = ((randomPtr.posrandom_generate() % 5) + 1) * 100;
		int price = ((randomPtr.posrandom_generate() % 5) * 10) + 50;
		if (t == 0) {
		    status = managerPtr.manager_addCar(wrapper, id, num, price);
		} else if (t == 1) {
		    status = managerPtr.manager_addFlight(wrapper, id, num, price);
		} else if (t == 2) {
		    status = managerPtr.manager_addRoom(wrapper, id, num, price);
		} else if (t == 3) {
		    status = managerPtr.manager_addCustomer(wrapper, id);
		}
		assert (status);
	    }

	} /* for t */

	boolean successful = false;
	while (!successful) {
	    try {
		wrapper.put(null, "MANAGER", managerPtr);
		successful = true;
	    } catch (Throwable e) {
		log.warn(e);
	    }
	}

	System.gc();
    }

}
