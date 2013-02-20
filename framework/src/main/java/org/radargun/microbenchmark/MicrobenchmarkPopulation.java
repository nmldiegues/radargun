package org.radargun.microbenchmark;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.microbenchmark.domain.IntSet;
import org.radargun.microbenchmark.domain.IntSetLinkedList;
import org.radargun.microbenchmark.domain.IntSetSkipList;

public class MicrobenchmarkPopulation {

    private static Log log = LogFactory.getLog(MicrobenchmarkPopulation.class);

    private final CacheWrapper wrapper;
    private final int items;
    private final int range;
    private final String set;

    public MicrobenchmarkPopulation(CacheWrapper wrapper, int items, int range, String set) {
	this.wrapper = wrapper;
	this.items = items;
	this.range = range;
	this.set = set;
    }

    public void performPopulation(){

	Random random = new Random();
	IntSet set = null;
	if (set.equals("ll")) {
	    set = new IntSetLinkedList(wrapper);
	} else if (set.equals("sl")) {
	    set = new IntSetSkipList(wrapper);
	}
	for (int i = 0; i < items; i++)
	    set.add(wrapper, random.nextInt(range));

	boolean successful = false;
	while (!successful) {
	    try {
		wrapper.put(null, "SET", set);
		successful = true;
	    } catch (Throwable e) {
		log.warn(e);
	    }
	}

	System.gc();
    }

}
