package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;
import org.radargun.tpcc.domain.CustomerLookup;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Note: the code is not fully-engineered as it lacks some basic checks (for example on the number
 *  of threads).
 *
 * @author Diego Didona <didona@gsd.inesc-id.pt>
 * @author Pedro Ruivo      
 */
public class ThreadParallelTpccPopulation extends TpccPopulation{

   private static Log log = LogFactory.getLog(ThreadParallelTpccPopulation.class);
   private static final long MAX_SLEEP_BEFORE_RETRY = 30000; //30 seconds

   protected int parallelThreads = 4;
   private int elementsPerBlock = 100;  //items loaded per transaction
   private AtomicLong waitingPeriod;

   public ThreadParallelTpccPopulation(CacheWrapper wrapper, int numWarehouses, int slaveIndex, int numSlaves,
                                       long cLastMask, long olIdMask, long cIdMask,
                                       int parallelThreads, int elementsPerBlock) {
      super(wrapper, numWarehouses, slaveIndex, numSlaves, cLastMask, olIdMask, cIdMask);
      this.parallelThreads = parallelThreads;
      this.elementsPerBlock = elementsPerBlock;

      if (this.parallelThreads <= 0) {
         log.warn("Parallel threads must be greater than zero. disabling parallel population");
         this.parallelThreads = 1;
      }
      if (this.elementsPerBlock <= 0) {
         log.warn("Batch level must be greater than zero. disabling batching level");
         this.elementsPerBlock = 1;
      }

      this.waitingPeriod = new AtomicLong(0);
   }

   @Override
   protected void populateItem(){
      log.trace("Populating Items");

      SplitIndex splitIndex = split(TpccTools.NB_MAX_ITEM, numberOfNodes, nodeIndex);

      performMultiThreadPopulation(splitIndex.start, splitIndex.end, parallelThreads, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateItemThread(lowerBound, upperBound);
         }
      });
   }

   @Override
   protected void populateStock(final int warehouseId){
      if (warehouseId < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + warehouseId);

      SplitIndex splitIndex = split(TpccTools.NB_MAX_ITEM, this.numberOfNodes, this.nodeIndex);

      performMultiThreadPopulation(splitIndex.start, splitIndex.end, parallelThreads, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateStockThread(lowerBound, upperBound, warehouseId);
         }
      });
   }

   @Override
   protected void populateDistricts(int warehouseId) {
      if (warehouseId < 0) {
         log.warn("Trying to populate Districts for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating District for warehouse " + warehouseId);

      SplitIndex splitIndex = split(TpccTools.NB_MAX_DISTRICT, this.numberOfNodes, this.nodeIndex);
      logDistrictPopulation(warehouseId, splitIndex.start, splitIndex.end);
      for (long districtId = splitIndex.start; districtId < splitIndex.end; ++districtId) {
         txAwarePut(createDistrict((int) districtId, warehouseId));
      }

      for (int districtId = 1; districtId <= TpccTools.NB_MAX_DISTRICT; ++districtId) {
         populateCustomers(warehouseId, districtId);
         populateOrders(warehouseId, districtId);
      }
   }

   @Override
   protected void populateCustomers(final int _warehouseId, final int _districtId){
      if (_warehouseId < 0 || _districtId < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Customers for warehouse " + _warehouseId + " and district " + _districtId);

      final ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance =
            new ConcurrentHashMap<CustomerLookupQuadruple, Integer>();

      SplitIndex splitIndex = split(TpccTools.NB_MAX_CUSTOMER, this.numberOfNodes, this.nodeIndex);

      performMultiThreadPopulation(splitIndex.start, splitIndex.end, parallelThreads, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateCustomerThread(lowerBound, upperBound, _warehouseId, _districtId, lookupContentionAvoidance);
         }
      });

      if(isBatchingEnabled()){
         populateCustomerLookup(lookupContentionAvoidance);
      }
   }

   protected void populateCustomerLookup(ConcurrentHashMap<CustomerLookupQuadruple, Integer> map){
      log.trace("Populating customer lookup ");

      final Vector<CustomerLookupQuadruple> vec_map = new Vector<CustomerLookupQuadruple>(map.keySet());
      long totalEntries = vec_map.size();

      log.trace("Populating customer lookup. Size is " + totalEntries);

      performMultiThreadPopulation(0, totalEntries, parallelThreads, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateCustomerLookupThread(lowerBound, upperBound, vec_map);
         }
      });
   }

   @Override
   protected void populateOrders(final int warehouseId, final int districtId){
      if (warehouseId < 0 || districtId < 0) {
         log.warn("Trying to populate Order with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Orders for warehouse " + warehouseId + " and district " + districtId);
      this._new_order = false;

      SplitIndex splitIndex = split(TpccTools.NB_MAX_ORDER, this.numberOfNodes, this.nodeIndex);

      performMultiThreadPopulation(splitIndex.start, splitIndex.end, parallelThreads, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateOrderThread(lowerBound, upperBound, warehouseId, districtId);
         }
      });
   }

   /*
    * ######################################### POPULATING THREADS ################################
    */

   protected class PopulateOrderThread extends PopulationThread {
      private final int warehouseId;
      private final int districtId;

      public PopulateOrderThread(long l, long u, int w, int d){
         super(l, u, elementsPerBlock);
         this.districtId = d;
         this.warehouseId = w;
      }

      @Override
      protected void executeTransaction(long start, long end) {
         logOrderPopulation(warehouseId, districtId, start, end);
         LinkedList<Integer> seqAleaList = new LinkedList<Integer>();
         boolean useList = false;

         do {
            startTransactionIfNeeded();
            Iterator<Integer> iterator = seqAleaList.iterator();

            for(long orderId=start; orderId <= end; orderId++){

               int generatedSeqAlea;

               if (useList && iterator.hasNext()) {
                  generatedSeqAlea = iterator.next();
               } else {
                  generatedSeqAlea = generateSeqAlea(0, TpccTools.NB_MAX_CUSTOMER-1);
                  seqAleaList.add(generatedSeqAlea);
               }

               int o_ol_cnt = tpccTools.aleaNumber(5, 15);
               Date aDate = new Date((new java.util.Date()).getTime());

               if (!txAwarePut(createOrder(orderId, districtId, warehouseId, aDate, o_ol_cnt, generatedSeqAlea))) {
                  break; // rollback tx
               }
               populateOrderLines(warehouseId, districtId, (int)orderId, o_ol_cnt, aDate);

               if (orderId >= TpccTools.LIMIT_ORDER){
                  populateNewOrder(warehouseId, districtId, (int)orderId);
               }
            }
            useList = true;
         } while (!endTransactionIfNeeded());
      }

      @Override
      public String toString() {
         return "PopulateOrderThread{" +
               "warehouseId=" + warehouseId +
               ", districtId=" + districtId +
               ", " + super.toString();
      }
   }

   protected class PopulateCustomerThread extends PopulationThread {
      private final int warehouseId;
      private final int districtId;
      private final ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance;

      @SuppressWarnings("unchecked")
      public PopulateCustomerThread(long lowerBound, long upperBound, int warehouseId, int districtId,
                                    ConcurrentHashMap c){
         super(lowerBound, upperBound, elementsPerBlock);
         this.districtId = districtId;
         this.warehouseId = warehouseId;
         this.lookupContentionAvoidance = c;
      }

      @Override
      protected void executeTransaction(long start, long end) {
         logCustomerPopulation(warehouseId, districtId, start, end);
         do {
            startTransactionIfNeeded();
            for(long customerId = start; customerId <= end; customerId++) {
               String c_last = c_last();

               if (!txAwarePut(createCustomer(warehouseId, districtId, customerId, c_last))) {
                  break; // rollback tx
               }

               if(isBatchingEnabled()){
                  CustomerLookupQuadruple clt = new CustomerLookupQuadruple(c_last,warehouseId,districtId, customerId);
                  if(!this.lookupContentionAvoidance.containsKey(clt)){
                     this.lookupContentionAvoidance.put(clt,1);
                  }
               } else{
                  CustomerLookup customerLookup = new CustomerLookup(c_last, warehouseId, districtId);
                  if (!txAwareLoad(customerLookup)) {
                     break; // rollback tx
                  }
                  customerLookup.addId(customerId);

                  if (!txAwarePut(customerLookup)) {
                     break; // rollback tx
                  }
               }

               populateHistory((int)customerId, warehouseId, districtId);
            }
         } while (!endTransactionIfNeeded());
      }

      @Override
      public String toString() {
         return "PopulateCustomerThread{" +
               "warehouseId=" + warehouseId +
               ", districtId=" + districtId +
               ", " + super.toString();
      }
   }

   protected class PopulateItemThread extends PopulationThread {

      public PopulateItemThread(long low, long up){
         super(low, up, elementsPerBlock);
      }

      @Override
      protected void executeTransaction(long start, long end) {
         logItemsPopulation(start, end);
         do {
            startTransactionIfNeeded();
            for(long itemId = start; itemId <= end; itemId++){
               if (!txAwarePut(createItem(itemId))) {
                  break; //rollback tx;
               }
            }
         } while (!endTransactionIfNeeded());
      }

      @Override
      public String toString() {
         return "PopulateItemThread{" + super.toString();
      }
   }

   protected class PopulateStockThread extends PopulationThread {
      private final int warehouseId;

      public PopulateStockThread(long low, long up, int warehouseId){
         super(low, up, elementsPerBlock);
         this.warehouseId = warehouseId;
      }

      @Override
      protected void executeTransaction(long start, long end) {
         logStockPopulation(warehouseId, start, end);
         do {
            startTransactionIfNeeded();
            for(long stockId = start; stockId <= end; stockId++){
               if (!txAwarePut(createStock(stockId, warehouseId))) {
                  break;
               }
            }
         } while (!endTransactionIfNeeded());
      }

      @Override
      public String toString() {
         return "PopulateStockThread{" +
               "warehouseId=" + warehouseId +
               ", " + super.toString();
      }
   }

   protected class PopulateCustomerLookupThread extends PopulationThread {
      private final Vector<CustomerLookupQuadruple> vector;

      @SuppressWarnings("unchecked")
      public PopulateCustomerLookupThread(long l, long u, Vector v){
         super(l, u, elementsPerBlock);
         this.vector = v;
      }

      @Override
      protected void executeTransaction(long start, long end) {
         logCustomerLookupPopulation(start, end);
         do {
            startTransactionIfNeeded();
            for(long idx = start; idx <= end; idx++){

               CustomerLookupQuadruple clq = this.vector.get((int)idx);
               CustomerLookup customerLookup = new CustomerLookup(clq.c_last, clq.warehouseId, clq.districtId);

               if (!txAwareLoad(customerLookup)) {
                  break; //rollback tx
               }

               customerLookup.addId(clq.customerId);

               if (!txAwarePut(customerLookup)) {
                  break; //rollback tx
               }
            }
         } while (!endTransactionIfNeeded());
      }

      @Override
      public String toString() {
         return "PopulateCustomerLookupThread{" + super.toString();
      }
   }

   protected class CustomerLookupQuadruple {
      private String c_last;
      private int warehouseId;
      private int districtId;
      private long customerId;


      public CustomerLookupQuadruple(String c, int w, int d, long i){
         this.c_last = c;
         this.warehouseId = w;
         this.districtId = d;
         this.customerId = i;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomerLookupQuadruple that = (CustomerLookupQuadruple) o;
         //The customer id does not count!!! it's not part of the key
         //if (customerId != that.customerId) return false;
         return districtId == that.districtId &&
               warehouseId == that.warehouseId &&
               !(c_last != null ? !c_last.equals(that.c_last) : that.c_last != null);

      }

      @Override
      public int hashCode() {
         int result = c_last != null ? c_last.hashCode() : 0;
         result = 31 * result + warehouseId;
         result = 31 * result + districtId;
         //I don't need customerId since it's not part of a customerLookup's key
         //result = 31 * result + (int)customerId;
         return result;
      }

      @Override
      public String toString() {
         return "CustomerLookupQuadruple{" +
               "c_last='" + c_last + '\'' +
               ", warehouseId=" + warehouseId +
               ", districtId=" + districtId +
               ", customerId=" + customerId +
               '}';
      }
   }

   public static abstract class PopulationThread extends Thread {
      private final long lowerBound;
      private final long upperBound;
      private final int elementsPerThread;

      protected PopulationThread(long lowerBound, long upperBound, int elementsPerThread) {
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         this.elementsPerThread = elementsPerThread;
      }

      @Override
      public void run() {
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerThread;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerThread;
         long base = lowerBound;

         for(long batch = 1; batch <= numBatches; batch++){
            long endBase = base + elementsPerThread - 1;
            logBatch(toString(), batch, numBatches, base, endBase);
            executeTransaction(base, endBase);
            base += elementsPerThread;
         }

         logRemainder(toString(), base, upperBound);
         executeTransaction(base, upperBound);

         logFinish(toString());
      }

      protected abstract void executeTransaction(long start, long end);

      @Override
      public String toString() {
         return "bounds=[" + lowerBound +"," + upperBound + "]}";
      }
   }

   protected final boolean isBatchingEnabled(){
      return this.elementsPerBlock != 1;
   }

   private void startTransactionIfNeeded() {
      if (isBatchingEnabled()) {
         //Pedro: this is experimental. I want to avoid the overloading of the network. 
         // So, instead of starting immediately the transaction, it waits a while
         long sleepFor = waitingPeriod.get();

         if (sleepFor > 0) {
            sleepFor(sleepFor);
         }
         wrapper.startTransaction();
      }
   }

   private boolean endTransactionIfNeeded() {
      if (!isBatchingEnabled()) {
         return true;
      }

      long start = System.currentTimeMillis();
      try {
         wrapper.endTransaction(true);
      } catch (Throwable t) {
         log.warn("Error committing transaction. Error is " + t.getMessage(), t);
         try {
            wrapper.endTransaction(false);
         } catch (Throwable t2) {
            //just ignore
         }
         sleepRandomly();
         log.warn("Retrying transaction...");
         return false;
      } finally {
         calculateNextWaitingTime(System.currentTimeMillis() - start);
      }
      return true;
   }

   private void calculateNextWaitingTime(long duration) {
      if (duration <= 10) {
         long old = waitingPeriod.get();
         waitingPeriod.set(old / 2);
         return ;
      }
      int counter = 0;
      while (duration > 0) {
         counter++;
         duration /= 10;
      }
      waitingPeriod.addAndGet(counter);
   }

   private void sleepRandomly() {
      Random r = new Random();
      long sleepFor;
      do {
         sleepFor = r.nextLong();
      } while (sleepFor <= 0);
      sleepFor(sleepFor % MAX_SLEEP_BEFORE_RETRY);
   }

   private void sleepFor(long milliseconds) {
      try {
         Thread.sleep(milliseconds);
      } catch (InterruptedException e) {
         //no-op
      }
   }

   private static void logStart(String thread) {
      log.debug("Starting " + thread);
   }

   private static void logFinish(String thread) {
      log.debug("Ended " + thread);
   }

   private static void logBatch(String thread, long batch, long numberOfBatches, long start, long end) {
      if (log.isDebugEnabled()) {
         log.debug(String.format("%s is populating the %s batch out of %s [%s,%s]",
                                 thread, batch, numberOfBatches, start, end));
      }
   }

   private static void logRemainder(String thread, long start, long end) {
      if (log.isDebugEnabled()) {
         log.debug(String.format("%s is populating the remainder [%s,%s]",
                                 thread, start, end));
      }
   }

   private void logCustomerLookupPopulation(long init, long end) {
      log.debug("Populate Customer Lookup from index " + init + " to " + end);
   }

   public static void performMultiThreadPopulation(long start, long end, int numberOfThreads,
                                                   ThreadCreator threadCreator) {
      Thread[] threads = new Thread[numberOfThreads];
      boolean trace = log.isTraceEnabled();

      //compute the number of item per thread
      long threadRemainder = (end - start) % numberOfThreads;
      long itemsPerThread = (end - start - threadRemainder) / numberOfThreads;

      if (trace) {
         log.trace(String.format("init=%s, nr of items=%s, nr of threads=%s, items per thread=%s, remainder=%s",
                                 start, end, numberOfThreads, itemsPerThread, threadRemainder));
      }

      long lowerBound = start;
      long itemsToAdd;

      for(int i = 1; i <= numberOfThreads; i++){
         itemsToAdd = itemsPerThread + (i == numberOfThreads ? threadRemainder : 0);
         if (trace) {
            log.trace(String.format("thread %s gets [%s,%s]", i, lowerBound, lowerBound + itemsToAdd - 1));
         }
         Thread thread = threadCreator.createThread(lowerBound, lowerBound + itemsToAdd - 1);
         threads[i-1] = thread;
         thread.start();
         lowerBound += (itemsToAdd);
      }

      //wait until all thread are finished
      try{
         for(Thread thread : threads){
            log.trace("Waiting for the end of " + thread);
            thread.join();
         }
         log.trace("All threads have finished! Movin' on");
      }
      catch(InterruptedException ie){
         ie.printStackTrace();
         System.exit(-1);
      }
   }

   public interface ThreadCreator {
      Thread createThread(long lowerBound, long upperBound);
   }
}
