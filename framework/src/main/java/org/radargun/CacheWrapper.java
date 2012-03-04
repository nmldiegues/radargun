package org.radargun;


import org.radargun.utils.TypedProperties;

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
     * caching product with various params passed in, described in
     * benchmark.xml for a particular caching product, which is
     * usually the name or path to a config file specific to the
     * caching product being tested.
     *
     * @param config
     * @param isLocal
     * @param nodeIndex
     * @param confAttributes
     * @throws Exception
     */
    void setUp(String config, boolean isLocal, int nodeIndex, TypedProperties confAttributes) throws Exception;

    /**
     * This is called at the very end of all tests on this cache, and is used for clean-up
     * operations.
     * @throws Exception
     */
    void tearDown() throws Exception;

    /**
     * This method is called when the framework needs to put an object in cache.  This method is treated
     * as a black box, and is what is timed, so it should be implemented in the most efficient (or most
     * realistic) way possible.
     *
     * @param bucket a bucket is a group of keys. Some implementations might ignore the bucket (e.g. InfinispanWrapper}}
     * so in order to avoid key collisions, one should make sure that the keys are unique even between different buckets.
     * @param key
     * @param value
     * @throws Exception
     */
    void put(String bucket, Object key, Object value) throws Exception;

    /**
     * @param bucket
     * @param key
     * @see #put(String, Object, Object)
     * @return
     * @throws Exception
     */
    Object get(String bucket, Object key) throws Exception;

    /**
     * This is called after each test type (if emptyCacheBetweenTests is set to true in benchmark.xml) and is
     * used to flush the cache.
     * @throws Exception
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
     * @param bucket
     * @param key
     */
    Object getReplicatedData(String bucket, String key) throws Exception;

    Object startTransaction();

    void endTransaction(boolean successful);

    public int size();

    Map<String, String> getAdditionalStats();

    /**
     *
     * @return
     */
    int getCacheSize();
}
