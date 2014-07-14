package org.robotninjas.barge.jaxrs.ws;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.robotninjas.barge.jaxrs.Jackson;
import org.robotninjas.barge.utils.Prober;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import java.net.URI;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import static org.robotninjas.barge.jaxrs.Logs.uniqueLog;


/**
 */
public class RaftJettyServerTest {

  private RaftJettyServer server;
  private URI uri;

  private final WebSocketClient wsClient = new WebSocketClient();
  private final Client client = ClientBuilder.newBuilder().register(Jackson.customJacksonProvider()).build();

  @Before
  public void setUp() throws Exception {
    this.server = new RaftJettyServer(0, new URI[] { new URI("http://localhost:12345") },
      uniqueLog()).start(0);
    this.uri = server.getPort();

    wsClient.start();
  }

  @After
  public void tearDown() throws Exception {
    this.wsClient.stop();
    this.server.stop();
  }

  @Test
  public void receives_START_event_through_web_socket_when_connecting_to_events_endpoint() throws Exception {
    URI wsEvents = new URI("ws://" + uri.getHost() + ":" + uri.getPort() + "/events");

    final Queue<String> messages = new LinkedBlockingQueue<>();
    final EventClientSocket socket = new EventClientSocket(messages);

    ClientUpgradeRequest request = new ClientUpgradeRequest();
    wsClient.connect(socket, wsEvents, request);

    client.target(uri).path("/raft/init").request().post(Entity.json(""));

    new Prober(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return Iterables.any(socket.messages, Predicates.contains(Pattern.compile(".*FOLLOWER.*")));
        }
      }).probe(10000);
  }


  @SuppressWarnings("UnusedDeclaration")
  @WebSocket(maxTextMessageSize = 64 * 1024)
  public static class EventClientSocket {

    private final Queue<String> messages;

    @SuppressWarnings("unused")
    private Session session;

    public EventClientSocket(Queue<String> messages) {
      this.messages = messages;
    }

    @OnWebSocketMessage
    public void onMessage(byte[] buffer, int offset, int size) {
      String message = new String(buffer, offset, size);
      this.messages.add(message);
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
      this.session = session;
    }

  }
}
