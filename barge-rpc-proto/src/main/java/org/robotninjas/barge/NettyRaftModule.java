package org.robotninjas.barge;

import com.google.inject.PrivateModule;

import org.robotninjas.barge.state.AbstractListenersModule;

import java.io.File;


public class NettyRaftModule extends PrivateModule {

  private final NettyClusterConfig config;
  private final File logDir;
  private final StateMachine stateMachine;
  private final long timeout;

  public NettyRaftModule(NettyClusterConfig config, File logDir, StateMachine stateMachine, long timeout) {
    this.config = config;
    this.logDir = logDir;
    this.stateMachine = stateMachine;
    this.timeout = timeout;
  }

  @Override
  protected void configure() {
    install(new AbstractListenersModule() {
        @Override
        protected void configureListeners() {
        }
      });

    install(RaftCoreModule.builder()
        .withTimeout(timeout)
        .withConfig(config)
        .withLogDir(logDir)
        .withStateMachine(stateMachine)
        .build());

    install(new RaftProtoRpcModule(config.local()));

    bind(NettyRaftService.class);
    expose(NettyRaftService.class);
  }

}
