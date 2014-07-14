package org.robotninjas.barge.jaxrs.ws;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;


/**
 */
public class EventSocket extends WebSocketAdapter {

  private final WsEventListener events;
  private final SessionToListener listenerFactory;

  private volatile Listener listener;

  public EventSocket(WsEventListener events) {
    this(events,new SessionToListener());
  }

  public EventSocket(WsEventListener events, SessionToListener listenerFactory) {
    this.events = events;
    this.listenerFactory = listenerFactory;
  }

  @Override
  public void onWebSocketConnect(Session sess) {
    super.onWebSocketConnect(sess);

    listener = listenerFactory.createListener(sess);

    events.addClient(listener);
  }

  @Override
  public void onWebSocketText(String message) {
    // do nothing
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    events.removeClient(listener);

    super.onWebSocketClose(statusCode, reason);
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    events.removeClient(listener);
    
    events.error(this, cause);
  }
}
