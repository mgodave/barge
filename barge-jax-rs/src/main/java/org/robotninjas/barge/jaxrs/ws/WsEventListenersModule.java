package org.robotninjas.barge.jaxrs.ws;

import com.google.inject.Singleton;

import org.robotninjas.barge.jaxrs.AbstractListenersModule;


/**
 */
public class WsEventListenersModule extends AbstractListenersModule {

  @Override
  protected void configureListeners() {
    bindListener().to(WsEventListener.class).in(Singleton.class);
  }
}
