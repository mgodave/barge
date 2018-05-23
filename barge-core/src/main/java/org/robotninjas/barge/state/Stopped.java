package org.robotninjas.barge.state;

import static org.robotninjas.barge.state.Raft.StateType.STOPPED;

import com.google.inject.Inject;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;

class Stopped extends BaseState {

  @Inject
  public Stopped(RaftLog log) {
    super(STOPPED, log);
  }

  @Override
  public void init(RaftStateContext ctx) {
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {
    throw new RuntimeException("Service is stopped");
  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {
    throw new RuntimeException("Service is stopped");
  }

  @Nonnull
  @Override
  public CompletableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) {
    CompletableFuture<Object> failed = new CompletableFuture<>();
    failed.completeExceptionally(new RaftException("Service is stopped"));
    return failed;
  }

}
