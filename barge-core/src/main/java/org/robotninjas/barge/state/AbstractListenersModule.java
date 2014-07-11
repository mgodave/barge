package org.robotninjas.barge.state;

import com.google.inject.AbstractModule;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;


/**
 * A container for making it possible to bind multiple instance of {@link org.robotninjas.barge.state.StateTransitionListener}.
 *
 * <p>This class uses Guice's {@link Multibinder} extension to make it possible to inject a collection of listeners
 * into the context. It is intended to be extended by Raft applicative level code to customize what implementations
 * of listeners are implemented:</p>
 * <pre>
 * public class MyListenersModule extends ListenersModule {
 *
 * }
 * </pre>
 *
 * @see <a href="https://code.google.com/p/google-guice/wiki/Multibindings">Guice Multibindings</a> on their wiki
 *
 * @see <a href="http://stackoverflow.com/questions/4410712/injecting-collection-of-classes-with-guice">Stack overflow</a>
 * question on the subject, along with another possible answer.
 */
public abstract class AbstractListenersModule extends AbstractModule {

  private Multibinder<StateTransitionListener> listenerBinder;
  private Multibinder<RaftProtocolListener> protocolListenerBinder;

  @Override
  protected void configure() {
    listenerBinder = Multibinder.newSetBinder(binder(), StateTransitionListener.class);
    protocolListenerBinder = Multibinder.newSetBinder(binder(), RaftProtocolListener.class);
    configureListeners();
  }

  protected abstract void configureListeners();

  protected final LinkedBindingBuilder<StateTransitionListener> bindTransitionListener() {
    return listenerBinder.addBinding();
  }
  
  protected final LinkedBindingBuilder<RaftProtocolListener> bindProtocolListener() {
    return protocolListenerBinder.addBinding();
  }

}
