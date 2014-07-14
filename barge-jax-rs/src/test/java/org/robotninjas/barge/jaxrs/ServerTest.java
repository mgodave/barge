package org.robotninjas.barge.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.robotninjas.barge.jaxrs.Logs.uniqueLog;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.utils.Prober;

import java.io.File;

import java.net.URI;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 */
public abstract class ServerTest<T extends RaftServer<T>> {

  @ClassRule
  public static MuteJUL muteJUL = new MuteJUL();

  private URI[] uris = new URI[3];

  private T httpServer1;
  private T httpServer2;
  private T httpServer3;

  @Before
  public void setUp() throws Exception {
    Logger.getLogger("").setLevel(Level.ALL);

    uris[0] = new URI("http://localhost:56789/");
    uris[1] = new URI("http://localhost:56790/");
    uris[2] = new URI("http://localhost:56791/");

    httpServer1 = createServer(0, uris, uniqueLog()).start(56789);
    httpServer2 = createServer(1, uris, uniqueLog()).start(56790);
    httpServer3 = createServer(2, uris, uniqueLog()).start(56791);
  }

  @After
  public void tearDown() throws Exception {
    httpServer1.stop();
    httpServer2.stop();
    httpServer3.stop();

    httpServer1.clean();
    httpServer2.clean();
    httpServer3.clean();
  }

  @Test
  public void can_commit_data_to_leader_instance() throws Exception {
    final Client client = ClientBuilder.newBuilder().register(Jackson.customJacksonProvider()).build();

    client.target(uris[0]).path("/raft/init").request().post(Entity.json(""));
    client.target(uris[1]).path("/raft/init").request().post(Entity.json(""));
    client.target(uris[2]).path("/raft/init").request().post(Entity.json(""));

    new Prober(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return isLeader(client, uris[0]) || isLeader(client, uris[1]) || isLeader(client, uris[2]);
        }
      }).probe(10000);

    URI leaderURI = getLeader(client);

    Response result = client.target(leaderURI)
        .path("/raft/commit")
        .request()
        .post(Entity.entity("foo".getBytes(),
            MediaType.APPLICATION_OCTET_STREAM));

    assertThat(result.getStatus()).isEqualTo(204);
  }

  protected abstract T createServer(int serverIndex, URI[] uris1, File logDir);

  private URI getLeader(Client client) {

    if (isLeader(client, uris[0]))
      return uris[0];

    if (isLeader(client, uris[1]))
      return uris[1];

    if (isLeader(client, uris[2]))
      return uris[2];

    throw new IllegalStateException("expected one server to be a leader");
  }

  private boolean isLeader(Client client, URI uri) {
    return client.target(uri).path("/raft/state").request().get(Raft.StateType.class).equals(Raft.StateType.LEADER);
  }

}
