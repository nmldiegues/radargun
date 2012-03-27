package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.CacheWrapper;

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
public class ThreadParallelTPCCPopulation extends TPCCPopulation{

   private static Log log = LogFactory.getLog(ThreadParallelTPCCPopulation.class);
   private static final long MAX_SLEEP_BEFORE_RETRY = 30000; //30 seconds

   private int parallelThreads = 4;
   private int elementsPerBlock = 100;  //items loaded per transaction
   
   private AtomicLong waitingPeriod;

   public ThreadParallelTPCCPopulation(CacheWrapper c, int slaveIndex, int numSlaves, boolean light, int parallelThreads, int batchLevel){
      super(c,slaveIndex,numSlaves,light);
      this.parallelThreads = parallelThreads;
      this.elementsPerBlock = batchLevel;

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

   protected void populate_item(){
      log.trace("Populating Items");

      long init_id_item = 1;
      long num_of_items = TPCCTools.NB_MAX_ITEM;

      if(numSlaves > 1){
         long remainder=TPCCTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TPCCTools.NB_MAX_ITEM-remainder)/numSlaves;

         init_id_item=(slaveIndex * num_of_items) + 1;

         if(slaveIndex==numSlaves-1){
            num_of_items+=remainder;
         }
      }

      performMultiThreadPopulation(init_id_item, num_of_items, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateItemThread(lowerBound, upperBound);
         }
      });
   }

   protected void populate_stock(final int id_warehouse){
      if (id_warehouse < 0) {
         log.warn("Trying to populate Stock for a negative warehouse ID. skipping...");
         return;
      }
      log.trace("Populating Stock for warehouse " + id_warehouse);

      long init_id_item=1;
      long num_of_items=TPCCTools.NB_MAX_ITEM;

      if(numSlaves>1){
         long remainder=TPCCTools.NB_MAX_ITEM % numSlaves;
         num_of_items=(TPCCTools.NB_MAX_ITEM-remainder)/numSlaves;

         init_id_item=(slaveIndex*num_of_items)+1;

         if(slaveIndex==numSlaves-1){
            num_of_items+=remainder;
         }
      }

      performMultiThreadPopulation(init_id_item, num_of_items, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateStockThread(lowerBound, upperBound, id_warehouse);
         }
      });
   }

   protected void populate_customer(final int id_warehouse, final int id_district){
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Customer with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Customers for warehouse " + id_warehouse + " and district " + id_district);

      final ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance = new ConcurrentHashMap<CustomerLookupQuadruple, Integer>();

      performMultiThreadPopulation(1, TPCCTools.NB_MAX_CUSTOMER, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateCustomerThread(lowerBound, upperBound, id_warehouse, id_district, lookupContentionAvoidance);
         }
      });

      if(isBatchingEnabled()){
         populate_customer_lookup(lookupContentionAvoidance);
      }
   }

   private void populate_customer_lookup(ConcurrentHashMap<CustomerLookupQuadruple,Integer> map){
      log.trace("Populating customer lookup ");

      final Vector<CustomerLookupQuadruple> vec_map = new Vector<CustomerLookupQuadruple>(map.keySet());
      long totalEntries = vec_map.size();

      log.trace("Populating customer lookup. Size is " + totalEntries);

      performMultiThreadPopulation(0, totalEntries, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateCustomerLookupThread(lowerBound, upperBound, vec_map);
         }
      });
   }

   protected void populate_order(final int id_warehouse, final int id_district){
      if (id_warehouse < 0 || id_district < 0) {
         log.warn("Trying to populate Order with a negative warehouse or district ID. skipping...");
         return;
      }

      log.trace("Populating Orders for warehouse " + id_warehouse + " and district " + id_district);
      this._new_order = false;

      performMultiThreadPopulation(1, TPCCTools.NB_MAX_ORDER, new ThreadCreator() {
         @Override
         public Thread createThread(long lowerBound, long upperBound) {
            return new PopulateOrderThread(lowerBound, upperBound, id_warehouse, id_district);
         }
      });
   }

   /*
    * ######################################### POPULATING THREADS ################################
    */

   private class PopulateOrderThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;
      private int id_district;

      @Override
      public String toString() {
         return "PopulateOrderThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               '}';
      }

      public PopulateOrderThread(long l, long u, int w, int d){

         this.lowerBound = l;
         this.upperBound = u;
         this.id_district = d;
         this.id_warehouse = w;
      }

      public void run(){
         logStart(toString());
         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder) / elementsPerBlock;
         long elementsPerBatch = elementsPerBlock;

         if(numBatches == 0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         long base = lowerBound;
         long toAdd;

         for(int j=1;j<=numBatches;j++){
            logBatch(toString(), j, numBatches);

            toAdd = elementsPerBatch + ((j==numBatches)? remainder:0);

            LinkedList<Integer> seqAleaList = new LinkedList<Integer>();
            boolean useList = false;

            do {
               startTransactionIfNeeded();

               Iterator<Integer> iterator = seqAleaList.iterator();

               for(long id_order=base;id_order<base+toAdd;id_order++){

                  int generatedSeqAlea;

                  if (useList && iterator.hasNext()) {
                     generatedSeqAlea = iterator.next();
                  } else {
                     generatedSeqAlea = generate_seq_alea(0, TPCCTools.NB_MAX_CUSTOMER-1);
                     seqAleaList.add(generatedSeqAlea);
                  }

                  int o_ol_cnt = TPCCTools.alea_number(5, 15);
                  Date aDate = new Date((new java.util.Date()).getTime());

                  Order newOrder= new Order(id_order,
                                            id_district,
                                            id_warehouse,
                                            generatedSeqAlea,
                                            aDate,
                                            (id_order < TPCCTools.LIMIT_ORDER)?TPCCTools.alea_number(1, 10):0,
                                            o_ol_cnt,
                                            1);

                  this.stubbornPut(newOrder);
                  populate_order_line(id_warehouse, id_district, (int)id_order, o_ol_cnt, aDate);

                  if (id_order >= TPCCTools.LIMIT_ORDER){
                     populate_new_order(id_warehouse, id_district, (int)id_order);
                  }
               }

               useList = true;
            } while (!endTransactionIfNeeded());
            base+=(toAdd);

         }
         logFinish(toString());

      }

      private void stubbornPut(Order o){
         boolean successful=false;
         while (!successful){
            try {
               o.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhilePut(o, e);
            }
         }
      }

   }

   private class PopulateCustomerThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;
      private int id_district;
      private ConcurrentHashMap<CustomerLookupQuadruple,Integer> lookupContentionAvoidance;

      @Override
      public String toString() {
         return "PopulateCustomerThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               '}';
      }

      @SuppressWarnings("unchecked")
      public PopulateCustomerThread(long lowerBound, long upperBound, int id_warehouse, int id_district,
                                    ConcurrentHashMap c){
         this.lowerBound = lowerBound;
         this.upperBound = upperBound;
         this.id_district = id_district;
         this.id_warehouse = id_warehouse;
         this.lookupContentionAvoidance = c;
      }

      public void run(){
         logStart(toString());
         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder)  / elementsPerBlock;

         long base = lowerBound;
         long toAdd;
         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }


         for(int j=1; j<=numBatches; j++){
            logBatch(toString(), j, numBatches);

            toAdd = elementsPerBatch + ((j==numBatches)? remainder:0);

            do {
               startTransactionIfNeeded();
               for(long i=base;i<base+toAdd;i++ ){

                  String c_last = c_last();
                  Customer newCustomer;
                  if(!light){
                     newCustomer=new Customer(id_warehouse,
                                              id_district,
                                              i,
                                              TPCCTools.alea_chainec(8, 16),
                                              "OE",
                                              c_last,
                                              TPCCTools.alea_chainec(10, 20),
                                              TPCCTools.alea_chainec(10, 20),
                                              TPCCTools.alea_chainec(10, 20),
                                              TPCCTools.alea_chainel(2, 2),
                                              TPCCTools.alea_chainen(4, 4) + TPCCTools.CHAINE_5_1,
                                              TPCCTools.alea_chainen(16, 16),
                                              new Date(System.currentTimeMillis()),
                                              (TPCCTools.alea_number(1, 10) == 1) ? "BC" : "GC",
                                              500000.0, TPCCTools.alea_double(0., 0.5, 4), -10.0, 10.0, 1, 0, TPCCTools.alea_chainec(300, 500));
                  }
                  else{
                     newCustomer=new Customer(id_warehouse,
                                              id_district,
                                              i,
                                              null,
                                              null,
                                              c_last,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              new Date(System.currentTimeMillis()),
                                              (TPCCTools.alea_number(1, 10) == 1) ? "BC" : "GC",
                                              500000.0, TPCCTools.alea_double(0., 0.5, 4), -10.0, 10.0, 1, 0, TPCCTools.alea_chainec(300, 500));
                  }
                  this.stubbornPut(newCustomer);

                  if(isBatchingEnabled()){
                     CustomerLookupQuadruple clt = new CustomerLookupQuadruple(c_last,id_warehouse,id_district, i);
                     if(!this.lookupContentionAvoidance.containsKey(clt)){
                        this.lookupContentionAvoidance.put(clt,1);
                     }
                  }
                  else{
                     CustomerLookup customerLookup = new CustomerLookup(c_last, id_warehouse, id_district);
                     stubbornLoad(customerLookup);
                     customerLookup.addId(i);
                     stubbornPut(customerLookup);
                  }

                  populate_history((int)i, id_warehouse, id_district);
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);
         }
         logFinish(toString());
      }


      private void stubbornPut(Customer c){
         boolean successful=false;
         while (!successful){
            try {
               c.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhilePut(c, e);
            }
         }
      }

      private void stubbornPut(CustomerLookup c){
         boolean successful=false;
         while (!successful){
            try {
               c.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhilePut(c, e);
            }
         }
      }

      private void stubbornLoad(CustomerLookup c){
         boolean successful=false;

         while (!successful){
            try {
               c.load(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhileGet(c, e);
            }
         }
      }
   }

   private class PopulateItemThread extends Thread{

      private long lowerBound;
      private long upperBound;

      @Override
      public String toString() {
         return "PopulateItemThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      public PopulateItemThread(long low, long up){
         this.lowerBound = low;
         this.upperBound = up;
      }

      public void run(){
         logStart(toString());

         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;
         long toAdd;

         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);

            toAdd = elementsPerBatch + ((batch==numBatches)? remainder:0);
            //Process a batch of elementsperBlock element

            do {
               startTransactionIfNeeded();
               for(long i=base; i<base+toAdd;i++){
                  Item newItem = new Item(i,
                                          TPCCTools.alea_number(1, 10000),
                                          TPCCTools.alea_chainec(14, 24),
                                          TPCCTools.alea_float(1, 100, 2),
                                          TPCCTools.s_data());
                  this.stubbornPut(newItem);
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);
         }
         logFinish(toString());
      }


      private void stubbornPut(Item newItem){
         boolean successful=false;
         while (!successful){
            try {
               newItem.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhilePut(newItem, e);
            }
         }
      }
   }

   private class PopulateStockThread extends Thread{
      private long lowerBound;
      private long upperBound;
      private int id_warehouse;

      @Override
      public String toString() {
         return "PopulateStockThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               ", id_warehouse=" + id_warehouse +
               '}';
      }

      public PopulateStockThread(long low, long up, int id_warehouse){
         this.lowerBound = low;
         this.upperBound = up;
         this.id_warehouse = id_warehouse;
      }

      public void run(){
         logStart(toString());
         //
         long remainder = (upperBound - lowerBound) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound - remainder ) / elementsPerBlock;
         long base = lowerBound;
         long toAdd;

         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);

            toAdd = elementsPerBatch + ((batch==numBatches)? remainder:0);
            //Process a batch of elementsperBlock element

            do {
               startTransactionIfNeeded();
               for(long i=base; i<base+toAdd;i++){
                  Stock newStock=new Stock(i,
                                           this.id_warehouse,
                                           TPCCTools.alea_number(10, 100),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           TPCCTools.alea_chainel(24, 24),
                                           0,
                                           0,
                                           0,
                                           TPCCTools.s_data());
                  this.stubbornPut(newStock);
               }
            } while (!endTransactionIfNeeded());
            base+=(toAdd);
         }
         logFinish(toString());

      }


      private void stubbornPut(Stock newStock){

         boolean successful=false;
         while (!successful){
            try {
               newStock.store(wrapper);
               successful = true;
            } catch (Throwable e) {
               logErrorWhilePut(newStock, e);
            }
         }

      }
   }

   private class PopulateCustomerLookupThread extends Thread{
      private Vector<CustomerLookupQuadruple> vector;
      private long lowerBound;
      private long upperBound;

      @Override
      public String toString() {
         return "PopulateCustomerLookupThread{" +
               "lowerBound=" + lowerBound +
               ", upperBound=" + upperBound +
               '}';
      }

      @SuppressWarnings("unchecked")
      public PopulateCustomerLookupThread(long l, long u, Vector v){
         this.vector = v;
         this.lowerBound = l;
         this.upperBound = u;
      }

      public void run(){
         logStart(toString());
         //I have to put +1  because it's inclusive
         long remainder = (upperBound - lowerBound  +1) % elementsPerBlock;
         long numBatches = (upperBound - lowerBound + 1 - remainder ) / elementsPerBlock;
         long base = lowerBound;
         long toAdd;

         long elementsPerBatch = elementsPerBlock;

         if(numBatches ==0){
            numBatches=1;
            elementsPerBatch = 0;  //there is only the remainder ;)
         }

         for(long batch = 1; batch <=numBatches; batch++){
            logBatch(toString(), batch, numBatches);
            toAdd = elementsPerBatch + ((batch==numBatches)? remainder:0);

            do {
               startTransactionIfNeeded();
               for(long i=base; i<base+toAdd;i++){

                  CustomerLookupQuadruple clq = this.vector.get((int)i);
                  CustomerLookup customerLookup = new CustomerLookup(clq.c_last, clq.id_warehouse, clq.id_district);
                  this.stubbornLoad(customerLookup);
                  customerLookup.addId(clq.id_customer);
                  this.stubbornPut(customerLookup);
               }
            } while (!endTransactionIfNeeded());
            base+=toAdd;
         }
         logFinish(toString());
      }


      private void stubbornPut(CustomerLookup c){

         boolean successful=false;
         while (!successful){
            try {
               c.store(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhilePut(c, e);
            }
         }

      }
      private void stubbornLoad(CustomerLookup c){
         boolean successful=false;

         while (!successful){
            try {
               c.load(wrapper);
               successful=true;
            } catch (Throwable e) {
               logErrorWhileGet(c, e);
            }
         }
      }

   }

   private class CustomerLookupQuadruple {
      private String c_last;
      private int id_warehouse;
      private int id_district;
      private long id_customer;


      public CustomerLookupQuadruple(String c, int w, int d, long i){
         this.c_last = c;
         this.id_warehouse = w;
         this.id_district = d;
         this.id_customer = i;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomerLookupQuadruple that = (CustomerLookupQuadruple) o;
         //The customer id does not count!!! it's not part of the key
         //if (id_customer != that.id_customer) return false;
         return id_district == that.id_district &&
               id_warehouse == that.id_warehouse &&
               !(c_last != null ? !c_last.equals(that.c_last) : that.c_last != null);

      }

      @Override
      public int hashCode() {
         int result = c_last != null ? c_last.hashCode() : 0;
         result = 31 * result + id_warehouse;
         result = 31 * result + id_district;
         //I don't need id_customer since it's not part of a customerLookup's key
         //result = 31 * result + (int)id_customer;
         return result;
      }

      @Override
      public String toString() {
         return "CustomerLookupQuadruple{" +
               "c_last='" + c_last + '\'' +
               ", id_warehouse=" + id_warehouse +
               ", id_district=" + id_district +
               ", id_customer=" + id_customer +
               '}';
      }
   }

   private boolean isBatchingEnabled(){
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

   private void logStart(String thread) {
      log.debug("Starting " + thread);
   }

   private void logFinish(String thread) {
      log.debug("Ended " + thread);
   }

   private void logBatch(String thread, long batch, long numberOfBatches) {
      log.debug(thread + " is populating the " + batch + " batch out of " + numberOfBatches);
   }

   private void logErrorWhilePut(Object object, Throwable throwable) {
      log.error("Error while trying to perform a put operation. Object is " + object +
                      ". Error is " + throwable.getLocalizedMessage() + ". Retrying...", throwable);
   }

   private void logErrorWhileGet(Object object, Throwable throwable) {
      log.error("Error while trying to perform a Get operation. Object is " + object +
                      ". Error is " + throwable.getLocalizedMessage() + ". Retrying...", throwable);
   }

   private void performMultiThreadPopulation(long initValue, long numberOfItems, ThreadCreator threadCreator) {
      Thread[] threads = new Thread[parallelThreads];

      //compute the number of item per thread
      long threadRemainder = numberOfItems % parallelThreads;
      long itemsPerThread = (numberOfItems - threadRemainder) / parallelThreads;

      long lowerBound = initValue;
      long itemsToAdd;

      for(int i = 1; i <= parallelThreads; i++){
         itemsToAdd = itemsPerThread + (i == parallelThreads ? threadRemainder:0);
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

   private interface ThreadCreator {
      Thread createThread(long lowerBound, long upperBound);
   }
}
