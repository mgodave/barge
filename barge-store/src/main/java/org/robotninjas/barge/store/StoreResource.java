package org.robotninjas.barge.store;

import javax.inject.Inject;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.created;
import javax.ws.rs.core.UriInfo;


@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class StoreResource {

  private RaftStore raftStore;

  @Inject
  private UriInfo uriInfo;

  @Inject
  public StoreResource(RaftStore raftStore) {
    this.raftStore = raftStore;
  }

  @Path("{key:.+}")
  @PUT
  @Consumes(APPLICATION_OCTET_STREAM)
  public Response store(@PathParam("key") String key, byte[] value) {
    byte[] bytes = raftStore.write(new Write(key, value));

    return created(uriInfo.getRequestUri()).type(APPLICATION_OCTET_STREAM).entity(bytes).build();
  }
}
