package org.robotninjas.barge.jaxrs.ws;

import java.util.List;
import org.robotninjas.barge.state.AbstractListenersModule;
import org.robotninjas.barge.state.RaftProtocolListener;
import org.robotninjas.barge.state.StateTransitionListener;


/**
 */
public class WsEventListenersModule extends AbstractListenersModule {

  private final List<StateTransitionListener> transitionListeners;
  private final List<RaftProtocolListener> protocolListeners;

  public WsEventListenersModule(List<StateTransitionListener> transitionListeners, List<RaftProtocolListener> protocolListeners) {
    this.transitionListeners = transitionListeners;
    this.protocolListeners = protocolListeners;
  }

  @Override
  protected void configureListeners() {

    for (StateTransitionListener listener : transitionListeners) {
      bindTransitionListener().toInstance(listener);
    }

    for (RaftProtocolListener protocolListener : protocolListeners) {
      bindProtocolListener().toInstance(protocolListener);
    }

  }
}
