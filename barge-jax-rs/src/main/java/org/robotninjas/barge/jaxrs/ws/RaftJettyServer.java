package org.robotninjas.barge.jaxrs.ws;

import com.google.common.base.Throwables;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.robotninjas.barge.jaxrs.RaftApplication;
import org.robotninjas.barge.jaxrs.RaftServer;

import java.io.IOException;
import java.net.URI;


/**
 * An instance for a Raft server using Jetty's embedded HTTP server.
 * <p>
 * This server exposes events of the lifecycle of Raft instance through a WebSocket API: Clients can connect to the
 * <tt>/events/</tt> URI and get notified in real-time of logged events.
 * </p>
 */
public class RaftJettyServer implements RaftServer<RaftJettyServer> {

  private final Server server;
  private final RaftApplication raftApplication;
  private final WsEventListener events;

  public RaftJettyServer(RaftApplication.Builder builder) {
    server = new Server();
    events = new WsEventListener();
    raftApplication = builder.setTransitionListeners(events).build();
  }

  public RaftJettyServer start(int port) {
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(port);
    server.addConnector(connector);

    // Setup the basic application "context" for this application at "/"
    // This is also known as the handler tree (in jetty speak)
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);

    // Add a websocket to a specific path spec
    ServletHolder holderEvents = new ServletHolder("ws-events", new EventServlet(events));
    context.addServlet(holderEvents, "/events/*");

    // Add Raft REST services endpoints
    context.addServlet(new ServletHolder(new ServletContainer(raftApplication.makeResourceConfig())), "/raft/*");

    try {
      events.start();
      server.start();

      return this;
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  public void stop() {

    try {
      raftApplication.stop();
      events.stop();
      server.stop();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public URI getPort() {
    return server.getURI();
  }

  @Override
  public void clean() {

    try {
      raftApplication.clean();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static class Builder {
    private RaftApplication.Builder builder;

    public Builder setApplicationBuilder(RaftApplication.Builder builder) {
      this.builder = builder;

      return this;
    }

    public RaftJettyServer build() {
      return new RaftJettyServer(builder);
    }

  }
}
