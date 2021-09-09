import java.io.Closeable;
import java.io.IOException;

// global variable to hold application level value like global cache, db connections etc
public class AppContext implements Closeable {

  // instance var

  private static AppContext appContext;

  private AppContext() {
    // init instance variable
  }

  public static synchronized AppContext getInstance() throws Exception {
    if (appContext == null) {
      appContext = new AppContext();
    }
    return appContext;
  }

  public String getCacheValue(){
    // removed complete code in sample code
    return  "";
  }

  @Override
  public void close() throws IOException {
    // clean resource like database connection etc
  }
}
