import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordArrayListener;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import javax.net.ssl.SSLContext;

public class UndertowTest implements HttpHandler {

  private final AppContext appContext;

  public UndertowTest(AppContext appContext) {
    this.appContext = appContext;
  }

  private static SSLContext createSSLContext() throws Exception {
    // commented only in sample code

//    KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
//    keyManagerFactory.init(keyStore, password);
//    KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

    SSLContext sslContext = SSLContext.getInstance("openssl.TLSv1.2");

    // commented only in sample code
//    sslContext.init(keyManagers, null, new SecureRandom());

    return sslContext;
  }

  /*
  it's server which receives request from another serves at very high rate
  the client servers periodically recycle (sometimes every 2-3 mins) existing KEEP ALIVE connections
  and establish new KEEP ALIVE connections (both http and https)
  it's high throughput (thousands of request/sec) and low latency system < 50 ms
   */
  public static void main(String[] args) throws Exception {
    // global context which holds application level value like global cache, db connections etc
    AppContext appContext = AppContext.getInstance();
    int cpu = Runtime.getRuntime().availableProcessors();
    int ioThreads = cpu;
    int workerThreads = cpu * 4;

    // commented only in sample code
//    SSLContext sslContext = createSSLContext();

    // start server instance to listen on port 80 and 443
    Undertow server = Undertow.builder()
        .addHttpListener(80, "0.0.0.0")
//        .addHttpsListener(443, "0.0.0.0", sslContext)  // commented only in sample code
        .setWorkerThreads(workerThreads)
        .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
        .setServerOption(UndertowOptions.IDLE_TIMEOUT, 150000) // 150000ms = 2.5m
        .setServerOption(UndertowOptions.NO_REQUEST_TIMEOUT, 150000) // 150000ms = 2.5m
        .setServerOption(org.xnio.Options.SSL_SERVER_SESSION_CACHE_SIZE, 1024 * 1000) // 1000000 sessions
        .setServerOption(org.xnio.Options.SSL_SERVER_SESSION_TIMEOUT, 150) // 2.5m
        .setIoThreads(ioThreads)
        .setWorkerOption(org.xnio.Options.TCP_NODELAY, true)
        .setSocketOption(org.xnio.Options.TCP_NODELAY, true)
        .setSocketOption(org.xnio.Options.KEEP_ALIVE, true)
        .setSocketOption(org.xnio.Options.REUSE_ADDRESSES, true)
        .setSocketOption(org.xnio.Options.CONNECTION_HIGH_WATER, 1_00_000) // to safeguard during periodic connection recycle by client serves
        .setSocketOption(org.xnio.Options.CONNECTION_LOW_WATER, 1_00_000)
        .setHandler(
            Handlers.routing()
                .post("/", new UndertowTest(appContext))
        )
        .build();
    server.start();
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {
    // read the complete request in IO thread and then pass it to worker thread
    exchange.getRequestReceiver().receiveFullBytes(
        (httpServerExchange, reqBytes) -> {
          // IO Thread ?
          httpServerExchange.dispatch(() -> {
            // Worker Thread ?
            new TestProcessor().process(httpServerExchange, reqBytes);
          });
        },
        (httpServerExchange, exception) -> {
          // IO Thread ?
          httpServerExchange.dispatch(() -> {
            // Worker Thread ?
            //optional error handler
            // e.g. Remote peer closed connection before all data could be read
            new TestProcessor().sendErrorResponse(httpServerExchange);
          });
        });
  }

  public void getBatch(Key[] keys, RecordArrayListener recordArrayListener) {
    // commented only in sample code
//    aerospikeClient.get(clientPolicy.eventLoops.next(), recordArrayListener, null, keys);
  }

  private class TestProcessor {

    public void process(HttpServerExchange httpServerExchange, byte[] reqBytes) {
      try {
        // parse JSON POST request body
        System.out.println(new String(reqBytes)); // only in sample code

        // do some more work
        String s = appContext.getCacheValue();

        // http sync call over network
        String response = httpSyncCall();

        // do some more work
        boolean someCondition = response.contains("some value") && s.contains("some other"); // s
        if (someCondition) {

          // async http call
          Key[] asKeys = new Key[10];

          getBatch(asKeys, new RecordArrayListener() {
            @Override
            public void onSuccess(Key[] keys, Record[] records) {
              // moving from NIO Event Loop to undertow worker thread
              httpServerExchange.dispatch(() -> {
                // In worker thread

                //parse response of callback

                someWork(httpServerExchange);

                // is it required?
                httpServerExchange.endExchange();
              });
            }

            @Override
            public void onFailure(AerospikeException exception) {
              // moving from NIO Event Loop to undertow worker thread
              httpServerExchange.dispatch(() -> {
                sendErrorResponse(httpServerExchange);
              });
            }
          });
        } else {
          someWork(httpServerExchange);

          // is it required?
          httpServerExchange.endExchange();
        }
      } catch (Exception e) {
        sendErrorResponse(httpServerExchange);
      }
    }

    public void someWork(HttpServerExchange httpServerExchange) {
      // do some work
      // some more work
      // send response to client
      String body = "some JSON response to client";
      httpServerExchange.setStatusCode(200);
      httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      httpServerExchange.getResponseSender().send(body);
    }

    public void sendErrorResponse(HttpServerExchange httpServerExchange) {
      // send error msg to client
      httpServerExchange.setStatusCode(204);
      httpServerExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
      httpServerExchange.getResponseSender().send("");

      // is it required?
      httpServerExchange.endExchange();
    }
  }

  private String httpSyncCall() {
    // actual call removed only in sample code
    return "";
  }
}
