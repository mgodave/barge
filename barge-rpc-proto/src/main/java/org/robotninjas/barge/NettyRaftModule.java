package org.robotninjas.barge;

import com.google.inject.PrivateModule;

import org.robotninjas.barge.RaftCoreModule.Builder;
import org.robotninjas.barge.state.AbstractListenersModule;

import java.io.File;
import java.util.concurrent.Executor;


public class NettyRaftModule extends PrivateModule {

  private final NettyClusterConfig config;
  private final File logDir;
  private final StateMachine stateMachine;
  private final long timeout;
  private final Executor executor;

  public NettyRaftModule(NettyClusterConfig config, File logDir, StateMachine stateMachine, long timeout, Executor executor) {
    this.config = config;
    this.logDir = logDir;
    this.stateMachine = stateMachine;
    this.timeout = timeout;
    this.executor = executor;
  }

  @Override
  protected void configure() {
    install(new AbstractListenersModule() {
        @Override
        protected void configureListeners() {
        }
      });

    Builder builder = RaftCoreModule.builder()
      .withTimeout(timeout)
      .withConfig(config)
      .withLogDir(logDir)
      .withStateMachine(stateMachine);

    if (executor != null) {
      builder.withExecutor(executor);
    }

    install(builder.build());

    install(new RaftProtoRpcModule(config.local()));

    bind(NettyRaftService.class);
    expose(NettyRaftService.class);
  }

}
