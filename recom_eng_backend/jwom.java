import java.io.*;
import java.util.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.common.*;
import org.apache.mahout.cf.taste.impl.recommender.svd.*;
import org.apache.mahout.cf.taste.impl.common.*;
import org.apache.mahout.cf.taste.recommender.*;


class jwom {

  public static void main (String argv[]) {
    //netflixRecommendAll();
    //movieLense();
    //WoMAgentRecommendAll();
    int numFeatures   = 50;
    int numIterations = 10;
    int spreadCountMin= 10;
    int threadCount   = 0;
    recommenderSVD.begin(numFeatures, numIterations, spreadCountMin, threadCount);
  }

  /* Use the recommender to recommend netflix movies.
  */
  public static void netflixRecommendAll () {
    final String DB_CSV = "ratings_data.csv";
    final String DB_TEST = "ratings_test.csv";
    FileReader     fReader=null;
    BufferedReader bReader=null;
    int i, len;
    long uid=0, cid;
    Timer gtimer = new Timer();
    Timer timer = new Timer();
    DataModel model = (new DataModelGenerator(DB_CSV)).model;
    String line, toks[];
    float rating = 0.0f;

    SVDPlusPlusFactorizer factorizer;
    SVDRecommender       recommender = null;

    try {
      factorizer = new SVDPlusPlusFactorizer(model, 50, 1);
      timer.reset("Prepared factorizer  numFeatures 10  numIterations 1. ");
      recommender = new SVDRecommender(model, factorizer);
      timer.reset("Prepared recommender. ");
    } catch (final TasteException e) { e.printStackTrace(); }

    // Print out recommendations for everyone.
    try {
      List<RecommendedItem> recs;
      LongPrimitiveIterator longiter = model.getUserIDs();
      while (longiter.hasNext()) {
        try {
          uid = longiter.next();
          recs = recommender.recommend(uid, 5);
          System.out.print(uid + ": " + recs + " ");
        } catch (final TasteException ex) {
          System.out.print("[" + uid + " invalid user]");
        }

        timer.reset("Recommended content. ");
      } // while
    } catch (final TasteException ex) {
        ex.printStackTrace();
    } // try

    gtimer.reset("Done and done. ");
    return;
  } // netflixRecommendAll()


  /* Compare netflix data estimates against actual user responses.  Currently 84% accurate.
  */
  public static void movieLense () {
    final String DB_CSV = "datasmall.csv";
    final String DB_TEST = "testsmall.csv";
    int numFeatures = 50;
    int numIterations = 10;
    FileReader     fReader=null;
    BufferedReader bReader=null;
    int i;
    Timer gtimer = new Timer();
    Timer timer = new Timer();
    DataModelGenerator dmg = new DataModelGenerator(DB_CSV);
    DataModel model = dmg.model;
    String line, toks[];
    int yesCount, guessCount=0, g; // Keep track of guess and iterate over them later.
    double split, percent, bestPercent=0.0, bestSplit=0.0;
    float guess, minGuess=10000.0f, maxGuess=-10000.0f;
    SVDRecommender recommender = null;

    System.out.print(dmg.db());

    float actuals[] = new float[500000];
    float guesses[] = new float[500000];

    /* Fire up the factorizer and recommender (with the the fs cache)
     */
    try {
      SVDPlusPlusFactorizer factorizer;
      System.out.println("Preparing recommender  numFeatures " + numFeatures + "  numIterations " + numIterations);
      factorizer = new SVDPlusPlusFactorizer(model, numFeatures, numIterations);
      timer.resetln("Prepared factorizer. ");
      recommender = new SVDRecommender(model, factorizer, new FilePersistenceStrategy(new File("netflixTestFile" + numFeatures + "_" + numIterations + "_" + DataModelGenerator.NormalizeKill + "_" + DataModelGenerator.NormalizeCutoff + "_" + DataModelGenerator.NormalizeSpread + ".mahout")));
      timer.resetln("Prepared recommender. ");
    } catch (final TasteException e) { e.printStackTrace(); }

    // Gather each user's actual rating and Mahout's predicted rating into two arrays.
    try {
      fReader = new FileReader(DB_TEST);
      bReader = new BufferedReader(fReader);
      while (null != (line = bReader.readLine())) {
        toks = line.split(",");
        try {
          guess = recommender.estimatePreference(Integer.parseInt(toks[0]), Integer.parseInt(toks[1]));
          if (guess < minGuess) { minGuess = guess; }
          if (maxGuess < guess) { maxGuess = guess; }
          guesses[guessCount] = guess;
          actuals[guessCount] = Float.parseFloat(toks[2]);
          ++guessCount;
        } catch (final TasteException e) {  }
      }
      bReader.close();
      fReader.close();
    } catch (final IOException ex) {
        ex.printStackTrace();
    } // try

    System.out.println("minGuess " + minGuess + "  maxGuess " + maxGuess);
    System.out.println("minValue " + DataModelGenerator.NormalizeKill +
                       "  cutoff " +  DataModelGenerator.NormalizeCutoff  +
                       "  maxValue " + DataModelGenerator.NormalizeSpread);

    // Sweep from the lowest to the highest recommend value.  Look for the sweet spot that gives the best binary recommendation split.
    for (split=minGuess; split<=maxGuess; split+=0.001) { // Split point
      yesCount=0;
      // Count the successful guesses at the current split.
      for (g=0; g<guessCount; ++g) {
        if ( (actuals[g] < DataModelGenerator.NormalizeCutoff ? false : true) ==
             (guesses[g] < split ? false : true)) {
          ++yesCount;
        }
      }
      percent = 100.0*((double)yesCount/(double)guessCount);
      if (bestPercent < percent) {
        bestPercent = percent;
        bestSplit = split;
      }
    }

    // Report the findings.
    System.out.print("Best split at " + bestSplit + " " + bestPercent + "%. ");
    gtimer.reset("Done and done. ");
    return;
  } // movieLense()


  /* Calculate recommendations for everyone from Postgres.  Prints five recommendations for each person.
  */
  public static void WoMAgentRecommendAll () {
    long uid=0;
    Timer timer = new Timer();
    DataModel                  model = (new DataModelGenerator(0)).model;
    SVDPlusPlusFactorizer factorizer;
    SVDRecommender       recommender = null;

    try {
      factorizer = new SVDPlusPlusFactorizer(model, 10, 1);
      timer.reset("Prepared factorizer (numFeatures 10  numIterations 1). ");
      recommender = new SVDRecommender(model, factorizer);
      timer.reset("Prepared recommender. ");
    } catch (final TasteException e) { e.printStackTrace(); }

    // Print out recommendations for everyone.
    try {
      List<RecommendedItem> recs;
      LongPrimitiveIterator itr = model.getUserIDs();
      while (itr.hasNext()) {
        try {
          recs = recommender.recommend(uid = itr.next(), 5);
          System.out.print(uid + ": " + recs + " ");
        } catch (final TasteException ex) {
          System.out.print("[" + uid + " invalid user]");
        }
        timer.reset("Recommended content. ");
      } // while
    } catch (final TasteException ex) {
      ex.printStackTrace();
    } // try

  } // WoMAgentRecommendAll()


} // class jwom

/*
String.startsWith(String)
String.equals(String)
String[] <= String.split(String)
Integer.parseInt(String)
Long <= System.currentTimeMillis()

// If a model store exists, parse the filename of the factorizer settings.
File d = new File(".");
for (File file: d.listFiles()) {
  toks = file.getName().split("\\.");
  if (4==toks.length && toks[0].equals("mahout")) System.out.println("Existing stores: " + file.getName());
}
*/
