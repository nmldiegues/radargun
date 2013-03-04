package org.radargun.stamp.vacation.transaction;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.Definitions;
import org.radargun.stamp.vacation.Random;
import org.radargun.stamp.vacation.domain.Manager;

public class MakeReservationOperation implements VacationTransaction {

    final private Manager manager;
    final private int[] types;
    final private int[] ids;
    final private int[] maxPrices;
    final private int[] maxIds;
    final private int customerId;
    final private int numQuery;

    public MakeReservationOperation(Manager manager, Random random, int numQueryPerTx, int queryRange) {
	this.manager = manager;
	this.types = new int[numQueryPerTx];
	this.ids = new int[numQueryPerTx];

	this.maxPrices = new int[Definitions.NUM_RESERVATION_TYPE];
	this.maxIds = new int[Definitions.NUM_RESERVATION_TYPE];
	this.maxPrices[0] = -1;
	this.maxPrices[1] = -1;
	this.maxPrices[2] = -1;
	this.maxIds[0] = -1;
	this.maxIds[1] = -1;
	this.maxIds[2] = -1;
	int n;
	this.numQuery = numQueryPerTx;
	this.customerId = random.posrandom_generate() % queryRange + 1;
	
	int[] baseIds = new int[20];
	for (int i = 0; i < 20; i++) {
	    baseIds[i] = (random.random_generate() % queryRange) + 1;
	}
	
	for (n = 0; n < numQuery; n++) {
	    types[n] = random.random_generate() % Definitions.NUM_RESERVATION_TYPE;
	    ids[n] = baseIds[n % 20];
	}
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {

	boolean isFound = false;
	int n;
	for (n = 0; n < numQuery; n++) {
	    int t = types[n];
	    int id = ids[n];
	    int price = -1;
	    if (t == Definitions.RESERVATION_CAR) {
		if (manager.manager_queryCar(cacheWrapper, id) >= 0) {
		    price = manager.manager_queryCarPrice(cacheWrapper, id);
		}
	    } else if (t == Definitions.RESERVATION_FLIGHT) {
		if (manager.manager_queryFlight(cacheWrapper, id) >= 0) {
		    price = manager.manager_queryFlightPrice(cacheWrapper, id);
		}
	    } else if (t == Definitions.RESERVATION_ROOM) {
		if (manager.manager_queryRoom(cacheWrapper, id) >= 0) {
		    price = manager.manager_queryRoomPrice(cacheWrapper, id);
		}
	    } else {
		assert (false);
	    }
	    if (price > maxPrices[t]) {
		maxPrices[t] = price;
		maxIds[t] = id;
		isFound = true;
	    }
	}

	if (isFound) {
	    manager.manager_addCustomer(cacheWrapper, customerId);
	}
	if (maxIds[Definitions.RESERVATION_CAR] > 0) {
	    manager.manager_reserveCar(cacheWrapper, customerId, maxIds[Definitions.RESERVATION_CAR]);
	}
	if (maxIds[Definitions.RESERVATION_FLIGHT] > 0) {
	    manager.manager_reserveFlight(cacheWrapper, customerId, maxIds[Definitions.RESERVATION_FLIGHT]);
	}
	if (maxIds[Definitions.RESERVATION_ROOM] > 0) {
	    manager.manager_reserveRoom(cacheWrapper, customerId, maxIds[Definitions.RESERVATION_ROOM]);
	}
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }

}