package org.robotninjas.barge.jaxrs.ws;

import org.eclipse.jetty.websocket.servlet.*;


/**
 */
public class EventServlet extends WebSocketServlet {
  private final WsEventListener events;

  public EventServlet(WsEventListener events) {
    this.events = events;
  }

  @Override
  public void configure(WebSocketServletFactory factory) {
    factory.setCreator(new WebSocketCreator() {
        @Override
        public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
          return new EventSocket(events);
        }
      });
  }
}
