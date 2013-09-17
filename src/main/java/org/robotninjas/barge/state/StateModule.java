package org.robotninjas.barge.state;

import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class StateModule extends PrivateModule {

  @Override
  protected void configure() {

    install(new FactoryModuleBuilder()
      .build(StateFactory.class));
    expose(StateFactory.class);

    bind(Context.class);
    expose(Context.class);

  }

}
