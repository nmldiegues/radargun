package org.radargun.microbenchmark;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.microbenchmark.domain.IntSet;
import org.radargun.microbenchmark.domain.IntSetLinkedList;
import org.radargun.microbenchmark.domain.IntSetSkipList;

public class MicrobenchmarkPopulation {

    private static Log log = LogFactory.getLog(MicrobenchmarkPopulation.class);

    private final CacheWrapper wrapper;
    private final int items;
    private final int range;
    private final String set;
    private final int clients;

    public MicrobenchmarkPopulation(CacheWrapper wrapper, int items, int range, String set, int clients) {
        this.wrapper = wrapper;
        this.items = items;
        this.range = range;
        this.set = set;
        this.clients = clients;
    }

    public void performPopulation(){
        
        Random random = new Random();
        for (int n = 0; n < clients; n++) {
            IntSet mySet = null;
            if (set.equals("ll")) {
                mySet = new IntSetLinkedList(n, wrapper);
            } else if (set.equals("sl")) {
                mySet = new IntSetSkipList(n, wrapper);
            }
            for (int i = 0; i < items; i++)
                mySet.add(wrapper, random.nextInt(range));

            boolean successful = false;
            while (!successful) {
                try {
                    LocatedKey key = wrapper.createKey("SET" + n, n);
                    wrapper.put(null, key, mySet);
                    successful = true;
                } catch (Throwable e) {
                    log.warn(e);
                }
            }
        }

        System.gc();
    }

}
