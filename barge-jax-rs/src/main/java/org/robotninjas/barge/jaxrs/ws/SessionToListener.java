package org.robotninjas.barge.jaxrs.ws;

import com.google.common.base.Throwables;
import org.eclipse.jetty.websocket.api.Session;

import java.io.IOException;

/**
 */
public class SessionToListener {
  public Listener createListener(Session sess) {
    return new WsSessionListener(sess);
  }

  private class WsSessionListener implements Listener {
    private final Session session;

    public WsSessionListener(Session session) {
      this.session = session;
    }

    @Override
    public void send(String message) {
      try {
        session.getRemote().sendString(message);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
