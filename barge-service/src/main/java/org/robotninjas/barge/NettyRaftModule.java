package org.robotninjas.barge;

import com.google.inject.PrivateModule;

import java.io.File;

public class NettyRaftModule extends PrivateModule {

  private final ClusterConfig config;
  private final File logDir;
  private final StateMachine stateMachine;
  private final long timeout;

  public NettyRaftModule(ClusterConfig config, File logDir, StateMachine stateMachine, long timeout) {
    this.config = config;
    this.logDir = logDir;
    this.stateMachine = stateMachine;
    this.timeout = timeout;
  }

  @Override
  protected void configure() {
    RaftCoreModule coreModule = new RaftCoreModule(config, logDir, stateMachine);
    coreModule.setTimeout(timeout);
    
    install(coreModule);
    install(new RaftProtoRpcModule(config.local()));

    bind(NettyRaftService.class);
    expose(NettyRaftService.class);
  }
  
}
