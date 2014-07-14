package org.robotninjas.barge.jaxrs.ws;

import com.google.common.base.Throwables;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.glassfish.jersey.servlet.ServletContainer;

import org.robotninjas.barge.jaxrs.RaftApplication;
import org.robotninjas.barge.jaxrs.RaftServer;
import org.robotninjas.barge.state.RaftProtocolListener;
import org.robotninjas.barge.state.StateTransitionListener;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;


/**
 * An instance for a Raft server using Jetty's embedded HTTP server.
 * <p>
 * This server exposes events of the lifecycle of Raft instance through a WebSocket API: Clients can connect to the
 * <tt>/events/</tt> URI and get notified in real-time of logged events.
 * </p>
 */
public class RaftJettyServer implements RaftServer<RaftJettyServer> {

  private static final String help = "Usage: java -jar barge-http.jar [options] <server index>\n" +
      "Options:\n" +
      " -h                       : Displays this help message\n" +
      " -c <configuration file>  : Use given configuration file for cluster configuration\n" +
      "                            This file is a simple property file with indices as keys and URIs as values, eg. like\n\n" +
      "                              0=http://localhost:1234\n" +
      "                              1=http://localhost:3456\n" +
      "                              2=http://localhost:4567\n\n" +
      "                            Default is './barge.conf'\n" +
      "<server index>            : Index of this server in the cluster configuration\n";

  private final Server server;
  private final RaftApplication raftApplication;
  private final WsEventListener events;

  public static void main(String[] args) throws IOException, URISyntaxException {
    muteJul();

    File clusterConfiguration = new File("barge.conf");
    int index = -1;

    for (int i = 0; i < args.length; i++) {

      switch (args[i]) {

        case "-c":
          clusterConfiguration = new File(args[++i]);

          break;

        case "-h":
          usage();
          System.exit(0);

        default:

          try {
            index = Integer.parseInt(args[i].trim());
          } catch (NumberFormatException e) {
            usage();
            System.exit(1);
          }

          break;
      }
    }

    if (index == -1) {
      usage();
      System.exit(1);
    }

    URI[] uris = readConfiguration(clusterConfiguration);

    RaftJettyServer server = new RaftJettyServer(index, uris, new File("log" + index)).start(uris[index].getPort());

    waitForInput();

    server.stop();

    System.out.println("Bye!");
    System.exit(0);
  }

  private static void waitForInput() throws IOException {
    //noinspection ResultOfMethodCallIgnored
    System.in.read();
  }

  private static void muteJul() {
    java.util.logging.Logger.getLogger("").setLevel(Level.ALL);
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private static URI[] readConfiguration(File clusterConfiguration) throws IOException, URISyntaxException {
    List<URI> uris = Lists.newArrayList();

    int lineNumber = 1;

    for (String line : CharStreams.readLines(new FileReader(clusterConfiguration))) {
      String[] pair = line.split("=");

      if (pair.length != 2)
        throw new IOException("Invalid cluster configuration at line " + lineNumber);

      uris.add(Integer.parseInt(pair[0].trim()), new URI(pair[1].trim()));
    }

    return uris.toArray(new URI[uris.size()]);
  }

  private static void usage() {
    System.out.println(help);
  }

  public RaftJettyServer(int serverIndex, URI[] uris, File logDir) {
    server = new Server();
    events = new WsEventListener();
    raftApplication = new RaftApplication(serverIndex, uris, logDir, Collections.<StateTransitionListener>singleton(events), Collections.<RaftProtocolListener>singleton(events));
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

}
