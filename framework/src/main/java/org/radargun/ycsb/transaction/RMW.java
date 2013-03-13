package org.radargun.ycsb.transaction;

import java.util.HashMap;
import java.util.Map;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.ycsb.ByteIterator;
import org.radargun.ycsb.RandomByteIterator;
import org.radargun.ycsb.StringByteIterator;
import org.radargun.ycsb.YCSB;
import org.radargun.ycsb.YCSBStressor;

public class RMW extends YCSBTransaction {

    private int k;
    private int multiplereadcount;
    private int random;
    private int recordCount;
    
    public RMW(int k, int random, int remote, int multiplereadcount, int recordCount) {
	super(random, remote);
	this.random = random;
	this.k = k;
	this.multiplereadcount = multiplereadcount;
	this.recordCount = recordCount;
    }

    @Override
    public void executeTransaction(CacheWrapper cacheWrapper) throws Throwable {
	HashMap<String,ByteIterator> values=new HashMap<String,ByteIterator>();

	for (int i=0; i< YCSB.fieldcount; i++) {
	    String fieldkey="field"+i;
	    ByteIterator data= new RandomByteIterator(YCSB.fieldlengthgenerator.nextInt());
	    values.put(fieldkey,data);
	}
	
	Map<String, String> row = StringByteIterator.getStringMap(values);
	int toWrite = (k * random) % multiplereadcount;
	for (int i = 0 ; i < multiplereadcount; i++) {
	    LocatedKey key = cacheWrapper.createKey("user" + (i % recordCount), super.node);
	    if (!super.remote && toWrite == i) {
		cacheWrapper.put(null, key, row);
	    } else {
		cacheWrapper.get(null, key);
	    }
	}
	
	if (remote) {
	    LocatedKey key = cacheWrapper.createKey("local" + super.node + "-" + YCSBStressor.THREADID.get(), super.node);
	    cacheWrapper.put(null, key, row);
	}
	
    }

    @Override
    public boolean isReadOnly() {
	return false;
    }
}
