package org.radargun;


//import org.infinispan.Cache;

import javax.transaction.RollbackException;
import java.util.Map;

/**
 * CacheWrappers wrap caching products tp provide RadarGun with a standard way of
 * accessing and manipulating a cache.
 *
 * @author Manik Surtani (manik@surtani.org)
 */
public interface CacheWrapper
{
    /**
     * Initialises the cache.  Typically this step will configure the
     * cacheing product with various params passed in, described in
     * benchmark.xml for a particular cacheing product, which is
     * usually the name or path to a config file specific to the
     * cacheing product being tested.
     *
     * @param config
     * @param nodeIndex
     */
    void setUp(String config, boolean isLocal, int nodeIndex) throws Exception;

    /**
     * This is called at the very end of all tests on this cache, and is used for clean-up
     * operations.
     */
    void tearDown() throws Exception;

    /**
     * This method is called when the framework needs to put an object in cache.  This method is treated
     * as a black box, and is what is timed, so it should be implemented in the most efficient (or most
     * realistic) way possible.
     *
     * @param bucket a bucket is a group of keys. Some implementations might ignore the bucket (e.g. {@link org.radargun.cachewrappers.InfinispanWrapper}}
     * so in order to avoid key collisions, one should make sure that the keys are unique even between different buckets.
     * @param key
     * @param value
     */
    void put(String bucket, Object key, Object value) throws Exception;

    /**
     * @see #put(String, Object, Object)
     */
    Object get(String bucket, Object key) throws Exception;

    /**
     * This is called after each test type (if emptyCacheBetweenTests is set to true in benchmark.xml) and is
     * used to flush the cache.
     */
    void empty() throws Exception;

    /**
     * @return the number of members in the cache's cluster
     */
    int getNumMembers();

    /**
     * @return Some info about the cache contents, perhaps just a count of objects.
     */
    String getInfo();

    /**
     * Some caches (e.g. JBossCache with  buddy replication) do not store replicated data directlly in the main
     * structure, but use some additional structure to do this (replication tree, in the case of buddy replication).
     * This method is a hook for handling this situations.
     */
    Object getReplicatedData(String bucket, String key) throws Exception;

    Object startTransaction();

    void endTransaction(boolean successful) throws RollbackException;


    boolean isPassiveReplication();


    boolean isPrimary();

    void switch_to_PC();

    void switch_to_PR();

    //Cache getCache();
    String getCacheMode();

    String getNodeName();

    void printNodes(Object key);

    boolean isLocal(Object key, int slice);

    boolean isFullyReplicated();

    public int getCacheSize();

    public Map<String, String> getAdditionalStats();
}
