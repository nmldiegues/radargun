package org.radargun.stages;

import org.radargun.CacheWrapper;
import org.radargun.DistStageAck;
import org.radargun.stamp.vacation.VacationPopulationStressor;

public class VacationPopulationStage extends AbstractDistStage {

    private int relations;
    private int shops;
    
    public void setRelations(int relations) {
	this.relations = relations;
    }
    
    public void setShops(int shops) {
    	this.shops = shops;
    }
    
    @Override
    public DistStageAck executeOnSlave() {
	DefaultDistStageAck ack = newDefaultStageAck();
	CacheWrapper wrapper = slaveState.getCacheWrapper();
	if (wrapper == null) {
	    log.info("Not executing any test as the wrapper is not set up on this slave ");
	    return ack;
	}

	long startTime = System.currentTimeMillis();
	populate(wrapper);
	long duration = System.currentTimeMillis() - startTime;
	log.info("The population took: " + (duration / 1000) + " seconds.");
	ack.setPayload(duration);
	return ack;
    }
    
    private void populate(CacheWrapper wrapper) {
	VacationPopulationStressor vacationStressor = new VacationPopulationStressor();
	vacationStressor.setRelations(relations);
	vacationStressor.setShops(shops);
	vacationStressor.stress(wrapper);
    }

}
