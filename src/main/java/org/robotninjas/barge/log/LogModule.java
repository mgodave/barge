package org.robotninjas.barge.log;

import com.google.inject.PrivateModule;

public class LogModule extends PrivateModule {

  @Override
  protected void configure() {

    bind(RaftLog.class).to(DefaultRaftLog.class).asEagerSingleton();
    expose(RaftLog.class);

  }

}
