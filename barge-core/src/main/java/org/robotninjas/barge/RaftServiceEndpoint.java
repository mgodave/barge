package org.robotninjas.barge;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.state.RaftStateContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static com.google.common.base.Strings.nullToEmpty;
import static org.robotninjas.barge.proto.RaftProto.*;

class RaftServiceEndpoint implements RaftProto.RaftService.Interface {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftServiceEndpoint.class);

  private final RaftStateContext ctx;

  public RaftServiceEndpoint(RaftStateContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void requestVote(@Nonnull RpcController controller, @Nonnull RequestVote request, @Nonnull RpcCallback<RequestVoteResponse> done) {
    try {
      done.run(ctx.requestVote(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing RequestVote", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  @Override
  public void appendEntries(@Nonnull RpcController controller, @Nonnull AppendEntries request, @Nonnull RpcCallback<AppendEntriesResponse> done) {
    try {
      done.run(ctx.appendEntries(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing AppendEntries", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  @Override
  public void installSnapshot(RpcController controller, SnapshotSegment request, RpcCallback<SnapshotSegmentResponse> done) {
    try {
      done.run(ctx.installSnapshot(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing InstallSnapshot", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }
}
