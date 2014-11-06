/** A simple console timer class.
 */
class Timer {
  private long mark;

  Timer () { mark =  System.currentTimeMillis(); }

  Timer (String str) {
    System.out.println(str);
    mark =  System.currentTimeMillis();
  }

  public String str () {
    long   current = System.currentTimeMillis();
    String s       = "[" + ((current - mark)/1000.0) + "s]";
    mark = current;
    return s;
  }

  private void _reset (String pre, String post) {
    System.out.print(pre + str() + post);
  }

  void resetln (String msg) { _reset(msg, "\n"); }
  void reset   (String msg) { _reset(msg, ""); }
  void reset   (String msg, String post) { _reset(msg, post); }

  void resetln () { resetln(""); }
  void reset   () { reset(""); }
}

