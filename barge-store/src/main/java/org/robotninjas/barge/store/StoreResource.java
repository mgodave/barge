package org.robotninjas.barge.store;

import com.google.common.base.Optional;

import javax.inject.Inject;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.created;
import static javax.ws.rs.core.Response.ok;
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

  @Path("{key:.+}")
  @GET
  @Produces(APPLICATION_OCTET_STREAM)
  public Response read(@PathParam("key") String key) {
    Optional<byte[]> bytes = raftStore.read(key);

    if (bytes.isPresent()) {
      return ok(bytes.get()).type(APPLICATION_OCTET_STREAM).build();
    }

    throw new NotFoundException("key " + key + " is not mapped in this store");
  }
}
