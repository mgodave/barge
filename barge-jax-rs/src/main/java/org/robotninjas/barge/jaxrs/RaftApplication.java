package org.robotninjas.barge.jaxrs;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.RaftProtocolListener;
import org.robotninjas.barge.state.StateTransitionListener;
import org.robotninjas.barge.utils.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;


/**
 */
public class RaftApplication {

  private static final Logger logger = LoggerFactory.getLogger(RaftJdkServer.class);

  private final int serverIndex;
  private final URI[] uris;
  private final File logDir;
  
  private final List<StateTransitionListener> transitionListeners;
  private final List<RaftProtocolListener> protocolListeners;

  private Optional<Injector> injector = Optional.absent();

  public RaftApplication(int serverIndex, URI[] uris, File logDir, Iterable<StateTransitionListener> transitionListener, Iterable<RaftProtocolListener> protocolListener) {
    this.serverIndex = serverIndex;
    this.uris = uris;
    this.logDir = logDir;
    this.transitionListeners = Lists.newArrayList(transitionListener);
    this.protocolListeners = Lists.newArrayList(protocolListener);
  }

  public RaftApplication(int serverIndex, URI[] uris, File logDir) {
    this(serverIndex,uris,logDir, Collections.<StateTransitionListener>emptyList(),Collections.<RaftProtocolListener>emptyList());
  }

  public ResourceConfig makeResourceConfig() {
    ClusterConfig clusterConfig = HttpClusterConfig.from(new HttpReplica(uris[serverIndex]),
      remotes());

    if (!logDir.exists() && !logDir.mkdirs())
      logger.warn("failed to create directories for storing logs, bad things will happen");

    StateMachine stateMachine = new StateMachine() {
      int i = 0;

      @Override
      public Object applyOperation(@Nonnull ByteBuffer entry) {
        return i++;
      }
    };

    final JaxRsRaftModule raftModule = new JaxRsRaftModule(clusterConfig, logDir, stateMachine, 1500, transitionListeners, protocolListeners);

    injector = Optional.of(Guice.createInjector(raftModule));

    ResourceConfig resourceConfig = new ResourceConfig();

    Binder binder = new AbstractBinder() {
      @Override
      protected void configure() {
        bindFactory(new Factory<Raft>() {
              @Override
              public Raft provide() {
                return injector.get().getInstance(Raft.class);
              }

              @Override
              public void dispose(Raft raft) {
              }
            }).to(Raft.class);
        
        bindFactory(new Factory<ClusterConfig>() {
          @Override public ClusterConfig provide() {
            return injector.get().getInstance(ClusterConfig.class);
          }

          @Override public void dispose(ClusterConfig instance) {
          }
        }).to(ClusterConfig.class);
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

  public void clean() throws IOException {
    Files.delete(logDir);
  }

  public void stop() {
    injector.transform(new Function<Injector, Object>() {
      @Nullable
      @Override
      public Object apply(@Nullable Injector input) {
        Raft instance = null;
        if (input != null) {
          instance = input.getInstance(Raft.class);
          instance.stop();
        }
        return instance;
      }
    });
  }
}
