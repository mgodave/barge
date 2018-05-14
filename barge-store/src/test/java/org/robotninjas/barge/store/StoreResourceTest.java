package org.robotninjas.barge.store;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.jetty.http.HttpStatus;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import javax.ws.rs.core.Response;


/**
 */
public class StoreResourceTest extends JerseyTest {

  private static final byte[] oldValue = { 0x41 };
  private static final byte[] value = { 0x42 };

  private static final Write write = new Write("foo", value);

  private RaftStore raftStore;

  @Test
  public void onPUTReturns201WithLocationAndPreviousValueGivenRaftStoreCompletesSuccessfully() throws Exception {
    when(raftStore.write(write)).thenReturn(oldValue);

    Response response = client().target("/store/foo").request().put(Entity.entity(value, APPLICATION_OCTET_STREAM_TYPE));

    assertThat(response.getStatus()).isEqualTo(HttpStatus.CREATED_201);
    assertThat(response.getHeaderString("Location")).isEqualTo("http://localhost:9998/store/foo");
    assertThat(response.readEntity(byte[].class)).isEqualTo(oldValue);
  }

  @Test
  public void onGETReturns200WithFoundValueGivenStoreContainsRequestedKey() throws Exception {
    when(raftStore.read("foo")).thenReturn(Optional.of(value));

    Response response = client().target("/store/foo").request().get(Response.class);

    assertThat(response.getStatus()).isEqualTo(HttpStatus.OK_200);
    assertThat(response.readEntity(byte[].class)).isEqualTo(value);
  }

  @Test
  public void onGETReturns404GivenStoreDoesNotContainRequestedKey() throws Exception {
    when(raftStore.read("foo")).thenReturn(Optional.<byte[]>absent());

    Response response = client().target("/store/foo").request().get(Response.class);

    assertThat(response.getStatus()).isEqualTo(HttpStatus.NOT_FOUND_404);
  }

  @Override
  protected Application configure() {
    raftStore = mock(RaftStore.class);

    ResourceConfig resourceConfig = ResourceConfig.forApplication(new Application() {
        @Override
        public Set<Object> getSingletons() {
          return Sets.newHashSet((Object) new StoreResource(raftStore));
        }
      });

    resourceConfig.register(OperationsSerializer.jacksonModule());

    return resourceConfig;
  }

}
