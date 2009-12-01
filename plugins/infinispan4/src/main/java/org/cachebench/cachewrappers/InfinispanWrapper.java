package org.cachebench.cachewrappers;

import org.cachebench.CacheWrapper;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import javax.transaction.TransactionManager;
import java.util.Map;

public class InfinispanWrapper implements CacheWrapper {

   private static Log log = LogFactory.getLog(InfinispanWrapper.class);
   CacheManager cacheManager;
   Cache<Object, Object> cache;
   TransactionManager tm;
   boolean started = false;
   String config;

   public void init(Map parameters) throws Exception {
      config = (String) parameters.get("config");
      setUp();
   }

   public void setUp() throws Exception {
      if (!started) {
         cacheManager = new DefaultCacheManager(config);
         // use the default cache
         cache = cacheManager.getCache();
         started = true;
      }
   }

   public void tearDown() throws Exception {
      if (started) {
         cacheManager.stop();
         started = false;
      }
   }

   public void put(String bucket, Object key, Object value) throws Exception {
      cache.put(bucket + key, value);
   }

   public Object get(String bucket, Object key) throws Exception {
      return cache.get(bucket + key);
   }

   public void empty() throws Exception {
      log.info("Cache size before clear: " + cache.size());
      cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).clear();
      log.info("Cache size after clear: " + cache.size());
   }

   public int getNumMembers() {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      if (componentRegistry.getStatus().startingUp()) {
         log.info("We're in the process of starting up.");
      }
      return cacheManager.getMembers() == null ? 0 : cacheManager.getMembers().size();
   }

   public String getInfo() {
      return cache.getVersion();
   }

   public Object getReplicatedData(String bucket, String key) throws Exception {
      return get(bucket, key);
   }

   public Object startTransaction() {
      if (tm == null) return null;
      try {
         tm.begin();
         return tm.getTransaction();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public void endTransaction(boolean successful) {
      if (tm == null) return;
      try {
         if (successful)
            tm.commit();
         else
            tm.rollback();
      }
      catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
