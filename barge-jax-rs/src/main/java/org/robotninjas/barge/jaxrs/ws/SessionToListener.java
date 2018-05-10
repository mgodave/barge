package org.robotninjas.barge.jaxrs.ws;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.eclipse.jetty.websocket.api.Session;


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
        session.getRemote().sendBytes(ByteBuffer.wrap(message.getBytes(Charsets.UTF_8)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String toString() {
      return "WsSession[" + session.getRemoteAddress() + "]";
    }
  }
}
