import java.io.*;
import java.util.*;
import java.sql.*;
import org.apache.mahout.cf.taste.model.*;
import org.apache.mahout.cf.taste.common.*;
import org.apache.mahout.cf.taste.impl.model.*;
import org.apache.mahout.cf.taste.impl.common.*;

/**
 * When instantiated, does all the work to create a Mahout GenericDataModel from either
 * a CSV file (passed in via a String) or the Postgres server (no arguments).
 *
 * dmg = new DataModelGenerator("file.scv")    Create from csv file
 * dmg = new DataModelGenerator(0)             Create from postgres DB
 * dmg.model                                   Get the Mahout DataModel
 */
class DataModelGenerator {

  // Public data
  public DataModel          model = null;
  public long      lastResponseID = 0;
  public long       responseCount = 0;

  public static final float NormalizeKill   =  1.0f;
  public static final float NormalizeCutoff =  3.0f;
  public static final float NormalizeSpread =  5.0f;

  //static final String DBurl = "jdbc:postgresql://db.freelogue.net:5432/ebdb";
  static final String DBurl = "jdbc:postgresql://dbproduction.freelogue.net:5432/ebdb";
  static final String DBuser = "recom_engine";
  static final String DBpass = "readonly";

  // Debugging
  String sourceStr;
  String queryTime = "n/a", totalTime;
  Timer  timer = new Timer();

  /**
   * Given a string rating, normalize to one of two float values: NormalizeKill or NormalizeSpread 
   * The recommender will consider a spread if "t" or over "3.0" and a kill if "f" or under "3.0"
   *
   * @param  rating  A string as a boolean "t" or "f" or a float "1.998".
   */
  float normalizeRating (String rating) {
    if (rating.equals("t")) return NormalizeSpread;
    if (rating.equals("f")) return NormalizeKill;
    return (Float.parseFloat(rating) < NormalizeCutoff) ? NormalizeKill : NormalizeSpread;
  }


  /** 
   * Create a DataModel via CSV file
   *
   * @param  csvFilename  The CSV filename
   */
  DataModelGenerator (String csvFilename) {
    FileReader     fReader = null;
    BufferedReader bReader = null;
    String line, toks[];
    ArrayList<GenericPreference>                   alist; // An array-list of apreference tuples (indexed via uid).
    HashMap<Integer, ArrayList<GenericPreference>> ahashmap = new HashMap<Integer, ArrayList<GenericPreference>>();
    FastByIDMap<PreferenceArray>                   preferences; // This gets initialied with the above ahashmap.
    long uid, cid;
    float response;

    try { // Read the file data into a map of user's and their responses
      sourceStr = csvFilename;
      fReader = new FileReader(csvFilename);
      bReader = new BufferedReader(fReader);

      while (null != (line = bReader.readLine())) {
        toks = line.split(",");
        uid    = Long.parseLong(toks[0]);
        cid    = Long.parseLong(toks[1]);
        response = normalizeRating(toks[2]);

        if (NormalizeKill < response) {
          // Consider this user's set of responses (or create it)
          alist = ahashmap.get((int)uid);
          if (null == alist) {
            alist = new ArrayList<GenericPreference>();
            ahashmap.put((int)uid, alist);
          }

          // Add response
          alist.add(new GenericPreference(uid, cid, response));
          ++responseCount;
        }
      }

      bReader.close();
      bReader.close();
    } catch (final IOException ex) {
        ex.printStackTrace();
    }

    // Create the official preference arrays based on our makeshift sets.
    preferences = new FastByIDMap<PreferenceArray>();
    for (Integer n: ahashmap.keySet()) {
      preferences.put(n, new GenericUserPreferenceArray(ahashmap.get(n)));
    }

    // Mahout data model all ready to use
    model = new GenericDataModel(preferences);
    totalTime = timer.str();
  } // DataModelGenerator()  via CSV file


  /** 
   * Create a DataModel via Postgres using default arguments.
   *
   * Considers only content with a spread count of 10 or more.
   *
   * @param  upToThisResponseID  Last table row to to read.
   */
  DataModelGenerator (long upToThisResponseID) {
    _DataModelGeneratorPostgres(upToThisResponseID, 10);
  }


  /** 
   * Create a DataModel via Postgres
   *
   * @param  upToThisResponseID  Last table row to to read.
   * @param  spreadCountMin      Number of spreads required to include a content in our data mode.
   */
  DataModelGenerator (long upToThisResponseID, long spreadCountMin) {
    _DataModelGeneratorPostgres (upToThisResponseID, spreadCountMin);
  }


  private void _DataModelGeneratorPostgres (long upToThisResponseID, long spreadCountMin) {
    ArrayList<GenericPreference>                   alist;
    HashMap<Integer, ArrayList<GenericPreference>> ahashmap = new HashMap<Integer, ArrayList<GenericPreference>>();
    FastByIDMap<PreferenceArray>                   preferences;
    long   uid, cid;
    float  response;
    Timer  timerQuery = new Timer();
    Connection conn = null;
    Statement st = null;
    ResultSet rs = null;

    // Open SQL connection and read user_responses into a temporary hashmap.
    try {
      Properties props = new Properties();
      props.setProperty("user",     DBuser);
      props.setProperty("password", DBpass);
      conn = DriverManager.getConnection(DBurl, props);

      if (0 == upToThisResponseID) {
        // Query for all responses
        sourceStr = "SELECT id, user_id, content_id, response FROM user_responses WHERE 2 <= user_id AND content_id in (SELECT id FROM contents WHERE " + spreadCountMin + " <= spread_count) ORDER BY id";
      } else {
        sourceStr = "SELECT id, user_id, content_id, response FROM user_responses WHERE 2 <= user_id AND id <= " + upToThisResponseID + " AND content_id in (SELECT id FROM contents WHERE " + spreadCountMin + " <= spread_count) ORDER BY id";
      }

      st = conn.createStatement();
      rs = st.executeQuery(sourceStr);
      queryTime = timerQuery.str();

      while (rs.next()) {
        lastResponseID = Integer.parseInt(rs.getString(1)); // Consider the user id
        uid = Integer.parseInt(rs.getString(2)); // Consider the user id
        cid = Integer.parseInt(rs.getString(3)); // Consider the content id
        response = normalizeRating(rs.getString(4));

        // Consider this user's set of responses or create it
        alist = ahashmap.get((int)uid);
        if (null == alist) { ahashmap.put((int)uid, alist = new ArrayList<GenericPreference>()); }

        // Add response
        alist.add(new GenericPreference(uid, cid, response));
        ++responseCount;
      }

    } catch (final SQLException ex) {
      System.out.println("WARNING:  SQLException: [" + ex.getCause() + "] [" + ex.getMessage() + "]");
      //ex.printStackTrace();
    } // try

      // Close SQL connection
    try { if (null != rs) rs.close(); } catch (SQLException ex) { }
    try { if (null != st) st.close(); } catch (SQLException ex) { }
    try { if (null != conn) conn.close(); } catch (SQLException ex) { }

    // Create the official preference arrays based on the temporary sets.
    preferences = new FastByIDMap<PreferenceArray>();
    for (Integer n: ahashmap.keySet()) {
      alist = ahashmap.get(n);
      preferences.put(n, new GenericUserPreferenceArray(alist));
    }

    // Mahout data model all ready to use
    model = new GenericDataModel(preferences);
    totalTime = timer.str();
  } // DataModelGenerator() (via Postgres)


  /** 
   * Debug dump useful stats in Technicolor.
   *
   */
  String db () {
    String db = "class DataModelGenerator\n";
      db += "  sourceStr         " + sourceStr + "\n";
      db += "  queryTime         " + queryTime + "\n";
      db += "  responseCount     " + responseCount + "\n";
    try {
      db += "  model.getNumUsers " + model.getNumUsers() + "\n";
      db += "  model.getNumItems " + model.getNumItems() + "\n";
      db += String.format("  Table density:    %2.2f%%", ((100.0 * responseCount) / (1.0 * model.getNumUsers() * model.getNumItems()))) + "\n";
    } catch (final TasteException e) { db += "ERROR: Unable to query model objet for users and/or items.\n"; }
    db += "  lastResponseID    " + lastResponseID + "\n";
    db += "  totalTime         " + totalTime;
    return db;
  }
  

} // class DataModelGenerator
