package org.robotninjas.barge.jaxrs.ws;

/**
 * A simple interface hiding the endpoint from Raft listener.
 */
public interface Listener {
  
  void send(String message);
}
