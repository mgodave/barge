package org.robotninjas.barge.store;

import com.google.common.collect.Sets;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Set;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;


public class ControlResourceTest extends JerseyTest {

  private Control control;

  @Override
  protected Application configure() {
    control = mock(Control.class);

    ResourceConfig resourceConfig = ResourceConfig.forApplication(new Application() {
        @Override
        public Set<Object> getSingletons() {
          return Sets.newHashSet((Object) new ControlResource(control));
        }
      });

    resourceConfig.register(OperationsSerializer.jacksonModule());

    return resourceConfig;
  }


  @Test
  public void onPOSTStopWithEmptyContentRequestsShutdownOfVM() throws Exception {
    client().target("/control/stop").request().post(Entity.entity("", MediaType.TEXT_PLAIN));

    verify(control).shutdown();
  }

}
