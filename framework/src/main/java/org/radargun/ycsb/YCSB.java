package org.radargun.ycsb;

import org.radargun.CacheWrapper;
import org.radargun.LocatedKey;
import org.radargun.ycsb.generators.CounterGenerator;
import org.radargun.ycsb.generators.DiscreteGenerator;
import org.radargun.ycsb.generators.IntegerGenerator;
import org.radargun.ycsb.generators.ZipfianGenerator;

public class YCSB {

    public static int fieldcount = 10;
    public static DiscreteGenerator operationchooser;
    public static CounterGenerator transactioninsertkeysequence;
    public static IntegerGenerator fieldlengthgenerator;

    public static final ThreadLocal<Integer> NODE_TARGET = new ThreadLocal<Integer>() {};

    public static final void put(CacheWrapper cacheWrapper, LocatedKey key, Object value) {
	try {
	    cacheWrapper.put(null, key, value);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	}
    }

    public static final <T> T get(CacheWrapper cacheWrapper, LocatedKey key) {
	try {
	    return (T) cacheWrapper.get(null, key);
	} catch (Exception e) {
	    if (e instanceof RuntimeException) {
		throw (RuntimeException)e;
	    }
	    e.printStackTrace();
	    return null;
	}
    }

    public static void preinit() {
	int fieldlength= 100;
	fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
    }
    
    public static void init(double readOnly, int recordCount) {
	operationchooser=new DiscreteGenerator();
	operationchooser.addValue(readOnly,"READ");
	operationchooser.addValue(1 - readOnly,"READMODIFYWRITE");
	
	transactioninsertkeysequence=new CounterGenerator(recordCount);
    }


}
