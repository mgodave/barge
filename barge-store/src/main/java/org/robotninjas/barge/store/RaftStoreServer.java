package org.robotninjas.barge.store;

import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import org.robotninjas.barge.jaxrs.Jackson;
import org.robotninjas.barge.jaxrs.RaftApplication;
import org.robotninjas.barge.jaxrs.ws.RaftJettyServer;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.logging.Level;

import javax.inject.Singleton;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;


/**
 * A standalone store accepting put/get operations through HTTP requests.
 *
 * <p>This is also the <tt>main</tt> class for the <tt>barge-store</tt> application. A {@link org.robotninjas.barge.store.RaftStoreServer} instnace
 * encapsulates an instance of {@link org.robotninjas.barge.jaxrs.ws.RaftJettyServer} which holds the main Jersey resources
 * context. This context is extended with a REST resource {@link org.robotninjas.barge.store.StoreResource} mapped to the URI
 * <tt>/raft/store/*</tt>, a {@link org.robotninjas.barge.store.RaftStore} implementation backed by a {@link org.robotninjas.barge.store.StoreStateMachine}
 * which holds the result of Barge's cluster operations.</p>
 *
 * <p>Within this implementation, writes are always sent to the cluster and reads always done against the current backing
 * state machine, implying that only <em>committed read</em> can ever be seen. </p>
 */
public class RaftStoreServer {

  private static final String help = "Usage: java -jar barge-store.jar [options] <server index>\n" +
      "Options:\n" +
      " -h                       : Displays this help message\n" +
      " -c <configuration file>  : Use given configuration file for cluster configuration\n" +
      "                            This file is a simple property file with indices as keys and URIs as values, eg. like\n\n" +
      "                              0=http://localhost:1234\n" +
      "                              1=http://localhost:3456\n" +
      "                              2=http://localhost:4567\n\n" +
      "                            Default is './barge.conf'\n" +
      "<server index>            : Index of this server in the cluster configuration\n";

  private static final ClusterConfiguration cluster = new ClusterConfiguration();

  private final int serverIndex;
  private final URI[] clusterURIs;
  private final File logDir;
  private RaftJettyServer server;

  public RaftStoreServer(int serverIndex, URI[] clusterURIs, File logDir) {
    this.serverIndex = serverIndex;
    this.clusterURIs = clusterURIs;
    this.logDir = logDir;
  }


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

    URI[] uris = cluster.readConfiguration(clusterConfiguration);

    final File logDir = new File("log" + index);

    RaftStoreServer server = new RaftStoreServer(index, uris, logDir);

    server.start(uris[index].getPort());
  }

  private static void muteJul() {
    java.util.logging.Logger.getLogger("").setLevel(Level.ALL);
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  private static void usage() {
    System.out.println(help);
  }

  public void start(int port) {
    final StoreStateMachine stateMachine = new StoreStateMachine();

    // Bridges our store objects to the DI context of the main RaftApplication context and Guice modules
    Binder binder = new AbstractBinder() {
      @Override
      protected void configure() {
        bind(stateMachine).to(StoreStateMachine.class);
        bind(RaftStoreInstance.class).to(RaftStore.class).in(Singleton.class);
      }
    };

    server =
      new RaftJettyServer.Builder().setApplicationBuilder(new RaftApplication.Builder() //
          .setServerIndex(serverIndex) //
          .setUris(clusterURIs) //
          .setLogDir(logDir) //
          .register(StoreResource.class) //
          .registerInstance(binder) //
          .setStateMachine(stateMachine)) //
      .build();

    server.start(port);

    // automatically call init on the registered raft instance
    init();
  }

  private void init() {
    final Client client = ClientBuilder.newBuilder().register(Jackson.customJacksonProvider()).build();
    client.target(clusterURIs[serverIndex]).path("/raft/init").request().post(Entity.json(""));
  }

  public void stop() {
    server.stop();
  }
}
