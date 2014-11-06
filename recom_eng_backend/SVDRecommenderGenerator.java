import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.common.*;
import org.apache.mahout.cf.taste.impl.recommender.svd.*;


class RecommenderContainer {
  public SVDRecommender recommender;
  public long           responseCount = 0;
  public long           lastResponseID = 0;
  public long           spreadCountMin = 0;
  public long           dob = 0;
  public RecommenderContainer (SVDRecommender _recommender, long _responseCount, long _lastResponseID, long _spreadCountMin, long _dob) {
    recommender    = _recommender;
    responseCount  = _responseCount;
    lastResponseID = _lastResponseID;
    spreadCountMin = _spreadCountMin;
    dob            = _dob;
  }
}


/** Setup pipeline that continuously generates a fresh SVD recommender.
 *
 *  After calling init(), getRecommenderContainer() to get the latest and greatest recommender().
 */
class SVDRecommenderGenerator implements Runnable {
  // Settings that control recommender creation
  private static int                 numFeatures;
  private static int                 numIterations;
  private static long                spreadCountMin = 0;    // Consider only content with at least this many spreads.  0 means everything.

  private static SVDRecommenderGenerator instances[];        // Keep track of all my threads
  private static long                durationAverage = 4*60*1000; // Assume a factorization duration time of 4 minutes

  private static RecommenderContainer recommenderContainer = null;
  private static long                 recommenderStart = 0;   // The time the current recommender was created (time factorization started)
  private static long                 recommenderDuration = 0;// The total time to create the current recommender (factorization, etc)
  private static int                  recommenderThread = 0;  // The thread ID that created the current recommender

  private static boolean             PowerOn = true;
  private static int                 staller = -1;        // Thread ID of the the last one to notice the DB has not changed and so blocks any other threads from attempting.
  private static Semaphore semaphore = new Semaphore(1);

  public static void sleep (long msec) { try { Thread.sleep(msec); } catch (final InterruptedException e) { } }

  public static void init (int _numFeatures, int _numIterations, int _spreadCountMin, int maxThreads) {
    numFeatures   = _numFeatures;
    numIterations = _numIterations;
    spreadCountMin = _spreadCountMin;

    if (maxThreads <= 0) {
      maxThreads = Runtime.getRuntime().availableProcessors();
      if (2 <= maxThreads) { --maxThreads; } // Spawn one less threads than there are CPUs (minimum of 1)
    }

    instances = new SVDRecommenderGenerator[maxThreads]; // Instantiate a new worker.
    for (int i=0; (i < maxThreads); ++i) { instances[i] = new SVDRecommenderGenerator(i); }
    for (int i=0; (i < maxThreads); ++i) { (new Thread(instances[i])).start(); }
  }

  public static void init (int numFeatures, int numIterations, int spreadCountMin) {
    init(numFeatures, numIterations, spreadCountMin, 0);
  }

  public static void init (int numFeatures, int numIterations) {
    init(numFeatures, numIterations, 0, 0);
  }

  public static void powerOff () { PowerOn = false; }

  public static boolean hasRecommender () { return recommenderContainer != null; }

  public static RecommenderContainer getRecommenderContainer () {
    while (null == recommenderContainer) sleep(1000);
    return recommenderContainer;
  }

  public static void dump (int id) {
    System.out.println("\n[1m!" + id + "! RecommenderPipeline  threads " + instances.length + "  numFeatures " + numFeatures + "  numIterations " + numIterations + "  durationAverage [" + durationAverage/1000 + "s]  recommender: " + recommenderThread + " [" + recommenderDuration/1000 + "s] " +  recommenderStart/1000 + " " + recommenderContainer.recommender);
    for (int i = 0; (i < instances.length); ++i) {
      DataModelGenerator dmg = instances[i].dmg;
      System.out.println("  " + i + " started at:" + ((dmg!=null)?instances[i].start/1000:-1) + "  SVDPlusPlusFactorizer " + instances[i].factorizerTime + "  SVDRecommender " + instances[i].recommenderTime);
    }
    System.out.print("[0m");
  }

  /* Thread stuff
  */

  private int                id;               // This thread's ID
  private long               start;
  private DataModelGenerator dmg = null;       // My DataModelGenerator
  private String             factorizerTime;
  private String             recommenderTime;

  // Constructor
  public SVDRecommenderGenerator (int _id) {
    id = _id;
  }

  // Update the current recommender with the newly updated one.  Verify its start time is later than the current.
  private void setRecommender (SVDRecommender rec) {
    try { semaphore.acquire();
      if ((null != rec) && (recommenderStart < start)) {  // Is this recommendation based on an older time than the current recommender?
        recommenderStart    = start;
        recommenderDuration = System.currentTimeMillis() - recommenderStart;
        recommenderThread   = id;
        durationAverage = (durationAverage + recommenderDuration) / 2;
        recommenderContainer = new RecommenderContainer(rec, dmg.responseCount, dmg.lastResponseID, spreadCountMin, recommenderStart/1000);
      } else {
        System.out.println("ERROR: Not updating recommender  (my start time " + start + " < current recommender's start time " + recommenderStart + ").");
      }
      semaphore.release();
    } catch (final InterruptedException ex) { }
  } 


  // Generate an SVD factorized matrix recommender
  private SVDRecommender buildRecommender (DataModelGenerator dmg) {
    Timer timer = new Timer();
    SVDPlusPlusFactorizer  factorizer;
    SVDRecommender         rec;

    System.out.print("\n!" + id + "! buildRecommender(" + numFeatures + ", " + numIterations  + ")");
    System.out.print("\n[33m!" + id + "!" + dmg.db() + "[0m");

    try {
      factorizer = new SVDPlusPlusFactorizer(dmg.model, numFeatures, numIterations);
      factorizerTime = timer.str();
      rec = new SVDRecommender(dmg.model, factorizer);
      recommenderTime = timer.str();
    } catch (final TasteException e) {
      e.printStackTrace();
      rec = null;
    }

    return rec;
  }


  // Return a current DataModel if its newer than the most recent thread and enough time has passed to build a new recommender.
  private DataModelGenerator getDataModelIfReadyToGenerate () {
    long lastStart = 0;
    DataModelGenerator lastDmg=null;

    if ((-1 != staller) && (staller != id)) return null;

    // Determine most recent running worker's dmg.
    for (int i=0; (i < instances.length); ++i) {
      DataModelGenerator d = instances[i].dmg;
      if ((null != d) && (lastStart < instances[i].start)) {
        lastStart = instances[i].start;
        lastDmg = d;
      }
    }

    // Look for latest time.  If none start immediately.  Otherwise sleep AverageWait/(CPU.count - 1) then start.
    if ((lastStart + durationAverage / instances.length) <= System.currentTimeMillis()) { // Is it time to generate a new model?
      start = System.currentTimeMillis();
      System.out.print("\n!"+id+"! getDataModelIfReadyToGenerate " + start/1000 + "  ");
      DataModelGenerator newDmg = (new DataModelGenerator(0, spreadCountMin)); // 0 = all rows
      if (null == lastDmg) return newDmg;
      if ((newDmg.lastResponseID != lastDmg.lastResponseID) &&
           (newDmg.responseCount != lastDmg.responseCount)) {
        staller = -1; // Remove pipeline stall.  All threads can query the DB for changes.
        return newDmg;
      } else {
        staller = id; // Stall pipeline.  Only this thread will check the DB for changes now.
      }
    }

    return null;
  }


  public void run () {
    DataModelGenerator newDmg;
    System.out.print("\n!"+id+"! SVDRecommenderGenerator::run()  Spawned.");
    while (PowerOn) {
      newDmg = null;
      try {
        semaphore.acquire();
        newDmg = getDataModelIfReadyToGenerate();
        if (null != newDmg) { dmg = newDmg; }
        semaphore.release();
      } catch (final InterruptedException ex) { }
      if (null == newDmg) {
        System.out.print("!"+id+"! sleeping...  ");
        sleep((id + 30) * 1000); // Try again in 30 second.
      } else {
        setRecommender(buildRecommender(dmg));
        dump(id);
      }
    }
    System.out.println("PowerOn = " + PowerOn + ".  Shutting down class SVDRecommenderGenerator.");
  }


} //class SVDRecommenderGenerator 
