package org.robotninjas.barge.state;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;
import static org.robotninjas.barge.state.Raft.StateType.START;

class Start extends BaseState {

  @Inject
  public Start(RaftLog log) {
    super(START, log);
  }

  @Override
  public void init(@Nonnull RaftStateContext ctx) {
    getLog().load();
    ctx.setState(this, FOLLOWER);
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {
    throw new RuntimeException("Service unavailable");
  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {
    throw new RuntimeException("Service unavailable");
  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    throw new RaftException("Service has not started yet");
  }

}
