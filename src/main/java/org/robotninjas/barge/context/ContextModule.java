package org.robotninjas.barge.context;

import com.google.inject.PrivateModule;

public class ContextModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(RaftContext.class).asEagerSingleton();
    expose(RaftContext.class);
  }

}
