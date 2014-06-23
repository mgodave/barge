package org.robotninjas.barge.store;

import com.google.common.base.Throwables;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.robotninjas.barge.jaxrs.Leaders;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;


/**
 */
public class StoreServerTest {

  private RaftStoreServer raftStoreServer;
  private URI[] clusterURIs;

  @Before
  public void startServer() throws URISyntaxException {
    clusterURIs = new URI[] { serverURI() };
    raftStoreServer = new RaftStoreServer(0, clusterURIs);

    raftStoreServer.start(56789);
  }

  @After
  public void stopServer() {
    raftStoreServer.stop();
  }

  @Test
  public void canWriteAndReadValuesInAStoreInAClusterOf1() throws Exception {
    final Client client = ClientBuilder.newBuilder().register(OperationsSerializer.jacksonModule()).build();

    new Leaders(clusterURIs).waitForALeader(client, 10000);

    client.target(serverURI()).path("/raft/store/foo").request().put(entity(new byte[] { 0x1 }, APPLICATION_OCTET_STREAM_TYPE));

    client.target(serverURI()).path("/raft/store/bar").request().put(entity(new byte[] { 0x2 }, APPLICATION_OCTET_STREAM_TYPE));

    byte[] bytes = client.target(serverURI()).path("/raft/store/foo").request().get(byte[].class);

    assertThat(bytes).isEqualTo(new byte[] { 0x1 });
  }

  private URI serverURI() {

    try {
      return new URI("http://localhost:56789/");
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }
}
