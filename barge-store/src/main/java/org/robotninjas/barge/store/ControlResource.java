package org.robotninjas.barge.store;

import javax.inject.Inject;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;


/**
 * Provides REST interface for controlling the store.
 */
@Path("/control")
public class ControlResource {

  private final Control control;

  @Inject
  public ControlResource(Control control) {
    this.control = control;
  }


  @POST
  @Path("/stop")
  @Consumes(MediaType.TEXT_PLAIN)
  public void stop() {
    control.shutdown();
  }
}
