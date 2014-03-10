package org.robotninjas.barge;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.state.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static com.google.common.base.Strings.nullToEmpty;
import static org.robotninjas.barge.proto.RaftProto.*;

class RaftServiceEndpoint implements RaftProto.RaftService.Interface {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftServiceEndpoint.class);

  private final Raft ctx;

  public RaftServiceEndpoint(Raft ctx) {
    this.ctx = ctx;
  }

  @Override
  public void requestVote(@Nonnull final RpcController controller, @Nonnull final RequestVote request, @Nonnull final RpcCallback<RequestVoteResponse> done) {
    try {
      done.run(ProtoUtils.convert(ctx.requestVote(ProtoUtils.convert(request))));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing RequestVote", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  @Override
  public void appendEntries(@Nonnull final RpcController controller, @Nonnull final AppendEntries request, @Nonnull final RpcCallback<AppendEntriesResponse> done) {
    try {
      done.run(ProtoUtils.convert(ctx.appendEntries(ProtoUtils.convert(request))));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing AppendEntries", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }
}
