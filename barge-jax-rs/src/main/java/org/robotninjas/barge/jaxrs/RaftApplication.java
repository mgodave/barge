package org.robotninjas.barge.jaxrs;

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import org.glassfish.jersey.server.ResourceConfig;

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.state.Raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.net.URI;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;


/**
 */
public class RaftApplication {


  private static final Logger logger = LoggerFactory.getLogger(RaftJdkServer.class);

  private final int serverIndex;
  private final URI[] uris;

  public RaftApplication(int serverIndex, URI[] uris) {
    this.serverIndex = serverIndex;
    this.uris = uris;
  }

  public ResourceConfig makeResourceConfig() {
    ClusterConfig clusterConfig = HttpClusterConfig.from(new HttpReplica(uris[serverIndex]),
      remotes());

    File logDir = new File("log" + serverIndex);

    if (!logDir.exists() && !logDir.mkdirs())
      logger.warn("failed to create directories for storing logs, bad things will happen");

    StateMachine stateMachine = new StateMachine() {
      int i = 0;

      @Override
      public Object applyOperation(@Nonnull ByteBuffer entry) {
        return i++;
      }
    };

    final JaxRsRaftModule raftModule = new JaxRsRaftModule(clusterConfig, logDir, stateMachine, 1500);

    final Injector injector = Guice.createInjector(raftModule);

    ResourceConfig resourceConfig = new ResourceConfig();

    Binder binder = new AbstractBinder() {
      @Override
      protected void configure() {
        bindFactory(new Factory<Raft>() {
              @Override
              public Raft provide() {
                return injector.getInstance(Raft.class);
              }

              @Override
              public void dispose(Raft raft) {
              }
            }).to(Raft.class);
      }
    };

    resourceConfig.register(BargeResource.class);
    resourceConfig.register(Jackson.customJacksonProvider());
    resourceConfig.register(binder);

    return resourceConfig;
  }

  private HttpReplica[] remotes() {
    HttpReplica[] remoteReplicas = new HttpReplica[uris.length - 1];

    for (int i = 0; i < remoteReplicas.length; i++) {
      remoteReplicas[i] = new HttpReplica(uris[(serverIndex + i + 1) % uris.length]);
    }


    return remoteReplicas;
  }
}
