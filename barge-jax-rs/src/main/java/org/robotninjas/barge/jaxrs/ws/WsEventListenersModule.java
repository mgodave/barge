package org.robotninjas.barge.jaxrs.ws;

import org.robotninjas.barge.jaxrs.AbstractListenersModule;
import org.robotninjas.barge.state.StateTransitionListener;

import java.util.List;


/**
 */
public class WsEventListenersModule extends AbstractListenersModule {

  private final List<StateTransitionListener> listeners;

  public WsEventListenersModule(List<StateTransitionListener> listeners) {
    this.listeners = listeners;
  }

  @Override
  protected void configureListeners() {

    for (StateTransitionListener listener : listeners) {
      bindListener().toInstance(listener);
    }
  }
}
