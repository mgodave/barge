package org.robotninjas.barge.jaxrs;

import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.utils.Prober;

import java.net.URI;

import java.util.concurrent.Callable;

import javax.ws.rs.client.Client;


/**
 * Utility to query for leader in a cluster and wait for cluster to reach a stable state with one leader.
 */
public class Leaders {

  private final URI[] uris;

  public Leaders(URI[] uris) {
    this.uris = uris;
  }

  public boolean isLeader(Client client, URI uri) {
    return client.target(uri).path("/raft/state").request().get(Raft.StateType.class).equals(Raft.StateType.LEADER);
  }

  /**
   * Retrieves the URI of the current leader in the cluster.
   *
   * @param client HTTP client to use
   * @return the URI of the current leader.
   * @throws  java.lang.IllegalStateException  if there is no current leader in the cluster.
   */
  public URI getLeader(Client client) {

    if (isLeader(client, uris[0]))
      return uris[0];

    if (isLeader(client, uris[1]))
      return uris[1];

    if (isLeader(client, uris[2]))
      return uris[2];

    throw new IllegalStateException("expected one server to be a leader");
  }

  /**
   * Wait a certain amount of time for a leader to emerge in this cluster.
   * @param client the HTTP client to use.
   * @param timeoutInMs timeout in milliseconds before giving up.
   */
  public void waitForALeader(final Client client, int timeoutInMs) {
    new Prober(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {

          for (URI uri : uris) {

            if (isLeader(client, uri))
              return true;
          }

          return false;
        }
      }).probe(timeoutInMs);
  }
}
