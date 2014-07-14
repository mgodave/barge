package org.robotninjas.barge.jaxrs.ws;

import org.eclipse.jetty.websocket.api.Session;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class EventSocketTest {

  private final WsEventListener events = mock(WsEventListener.class);
  private final Session session = mock(Session.class);
  private final Listener listener = mock(Listener.class);
  private final SessionToListener listenerFactory=  mock(SessionToListener.class);

  private final EventSocket socket = new EventSocket(events, listenerFactory);

  @Before
  public void setup(){
    when(listenerFactory.createListener(session)).thenReturn(listener);  
  }
  
  @Test
  public void registers_a_listener_when_sockets_open() throws Exception {
    socket.onWebSocketConnect(session);

    verify(events).addClient(listener);
  }

  @Test
  public void removes_registered_session_when_sockets_close() throws Exception {
    socket.onWebSocketConnect(session);
    socket.onWebSocketClose(200, "closing");

    verify(events).removeClient(listener);
  }

  @Test
  public void removes_registered_session_and_notifies_exception_when_sockets_has_error() throws Exception {
    Exception error = new Exception("error");

    socket.onWebSocketConnect(session);
    socket.onWebSocketError(error);

    verify(events).removeClient(listener);
    verify(events).error(socket, error);
  }

}