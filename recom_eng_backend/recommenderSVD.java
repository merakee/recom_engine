import java.net.*;
import java.io.*;
import java.util.*;
import org.apache.mahout.cf.taste.common.*;
import org.apache.mahout.cf.taste.impl.recommender.svd.*;
import org.apache.mahout.cf.taste.recommender.*;

/** Spawn (currently two) recommendation agent that continuously generates an updated recommender
 *  and computes recommendations based on the last generated Mahout recommender.
 */
class recommenderSVD implements Runnable {

  public static void begin (int numFeatures, int numIterations, int spreadCountMin, int threadCount) {
    // Start recommender generator pipline.
    SVDRecommenderGenerator.init(numFeatures, numIterations, spreadCountMin, threadCount);
    SVDRecommenderGenerator.getRecommenderContainer(); // Block suntil a recommender is available.
    // Spawn the listener thread loops
    System.out.print("\n::recommenderSVD.begin  Pipeline has generated its first recommender.");
    new Thread(new recommenderSVD(2014)).start();
    new Thread(new recommenderSVD(2015)).start();
  }

  public static void sleep (long msec) { try { Thread.sleep(msec); } catch (final InterruptedException e) { } }

  /* Thread stuff */

  private int port;

  public recommenderSVD (int _port) {
    port = _port;
  }

  /* Spawn new thread for each incomming connction.
   */
  public void run () {
    int id = 0;
    ServerSocket ssock = null;
    Socket sock;
    for (;;) {
      if (null == ssock) try {
          //System.out.print("\n::recommenderSVD(" + port + ")  Opening listening on local port " + port);
          ssock = new ServerSocket(port);
        } catch (IOException e) {
          //System.out.print("\n::recommenderSVD(" + port + ")  ServerSocket IOException.  Setting ssock = NULL.  Trying again in 5 second.");
          ssock = null;
          sleep(5 * 1000);
          continue;
        } // if try
      try {
        sock = ssock.accept();
        new Thread(new recommenderSVDWorker(id++, port, sock)).start(); // Spawn new recommendation thread
      } catch (IOException ex) {
          //System.out.print("\n::recommenderSVD(" + port + ")  accept IOException.  Setting ssock = NULL.  Trying again in 5 second.");
          ssock = null; // Should I try and close the listener socket 'ssock'?
          sleep(5 * 1000);
          continue;
      }
    } // for(;;)
  } // public void run()

} // class recommenderSVD


/**
 * Opens a local port and spawns new thread for each incoming recommendation request.
 */
class recommenderSVDWorker implements Runnable {
  private int               id; // This agent's unique ID
  private long             uid;
  private int            count;
  private int             port;
  private Socket          sock;
  private BufferedReader    in;
  private DataOutputStream out;
  public String            params; // The thing being processed

  // Constructor
  public recommenderSVDWorker (int _id, int _port, Socket _sock) {
    id = _id;
    port = _port;
    sock = _sock;
    try {
      out = new DataOutputStream(sock.getOutputStream());
      in  = new BufferedReader(new InputStreamReader(sock.getInputStream()));
      out.writeBytes("# RECOMMENDER SVD\n"); // Send a hello message.
    } catch (SocketException e) {
      System.out.print("\n  SocketException.  Setting in/out = null.");
      in = null;
      out = null;
    } catch (IOException e) {
      System.out.print("\n  IOException.  Setting in/out = null.");
      in = null;
      out = null;
    }
  }

  private String _getRecommendations (String line, SVDRecommender recommender) {
    String buff="[", toks[];
    List<RecommendedItem> recommendations;
    Iterator<RecommendedItem> iter;

    if (null == line) { return buff+"]"; }

    try {
      toks = line.split(" "); // Tokenize lien
      uid = Long.parseLong(toks[0]);
      count = (1 < toks.length) ? Integer.parseInt(toks[1]) : 10; // Default count is 10
      recommendations = recommender.recommend(uid, count); // Get recommendation
      System.out.print("\n recommender(" + uid + ", " + count + ") " + recommender + " => " + recommendations.size() + " items ");

      // Build ruby list of content IDs for the user.
      iter = recommendations.iterator();
      RecommendedItem rec;
      if (iter.hasNext()) { // Write first
        rec = iter.next();
        buff += "[" + rec.getItemID() + "," + rec.getValue() + "]";
      }

      while (iter.hasNext()) { // The the rest, if any
        rec = iter.next();
        buff += ",[" + rec.getItemID() + "," + rec.getValue() + "]";
      }

    } catch (final NumberFormatException ex) {
      System.out.print("\n  NumberFormatException:  uid " + uid + " invalid input.  ");
    } catch (final TasteException ex) {
      System.out.print("\n  TasteException:  uid " + uid + " is invalid.  ");
    } catch (final IllegalArgumentException ex) {
      System.out.print("\n  IllegalArgumentException:  count " + count + " is invalid.  ");
    } catch (final NullPointerException ex) {
      System.out.print("\n  NullPointerException:  setting buff = \"[]\"");
      buff = "[";
    } // try
    return buff + "]"; // Send the recommendation
  } // _getRecommendations 

  public void run () {
    String recs, result;
    long mark;
    for(;;) { // Run forever.
      result = "{}";
      mark = System.currentTimeMillis();
      RecommenderContainer recommenderContainer = SVDRecommenderGenerator.getRecommenderContainer(); // Consider latest recommenderContainer
      try {
        System.out.print("\n" + id + " readLine...");
        params = in.readLine(); // readLine will return a newline delimited string even if the last thing read from the socket was not newline delimited.
        System.out.print("\n" + id + " readLine => '" + params + "'");
        if (null == params) { throw new IOException(); }
      } catch (IOException ex) {
        System.out.print("\n" + id + " NULL or IO Exception during read");
        break;
      }
      if (2014==port) {
        result = _getRecommendations(params, recommenderContainer.recommender);
      } else if (2015 == port) {
        recs = _getRecommendations(params, recommenderContainer.recommender);
        result = "{:uid=>"             + uid
               + ",:count=>"           + count
               + ",:spreadCountMin=>"  + recommenderContainer.spreadCountMin
               + ",:elapsedTime=>"     + (System.currentTimeMillis() - mark)
               + ",:recommendations=>" + recs
               + ",:responseCount=>"   + recommenderContainer.responseCount
               + ",:lastResponseID=>"  + recommenderContainer.lastResponseID
               + ",:dob=>"             + recommenderContainer.dob
               + "}";
      } 
      System.out.print("\n" + id + "  writing " + result);
      try {
        out.writeBytes(result);
        out.writeBytes("\n");
        out.flush();
        System.out.print("\n" + id + " wrote and flushed.");
      } catch (IOException ex) {
        try { in.skip(Long.MAX_VALUE); } catch (IOException exx) { } // Clear the input buffer if we can't write any more.
        System.out.print("\n" + id + " IO Exception during write");
        break;
      }
    } // for(;;)

    System.out.print("\n" + id + "Closing ports and client socket...");
    try { in.close(); } catch (IOException ex) { }
    try { out.close(); } catch (IOException ex) { }
    try { sock.close(); } catch (IOException ex) { }
    System.out.print("\n" + id + "Thread exiting.");
  } // public void run ()

} // class recommenderSVDWorker

