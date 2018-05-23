package org.robotninjas.barge.state;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;


/**
 * Main interface to a Raft protocol instance.
 */
public interface Raft {

  enum StateType {
    START, FOLLOWER, CANDIDATE, LEADER, STOPPED
  }

  void addRaftProtocolListener(RaftProtocolListener protocolListener);

  CompletableFuture<StateType> init();

  @Nonnull CompletableFuture<RequestVoteResponse> requestVote(@Nonnull RequestVote request);

  @Nonnull CompletableFuture<AppendEntriesResponse> appendEntries(@Nonnull AppendEntries request);

  @Nonnull CompletableFuture<Object> commitOperation(@Nonnull byte[] op);

  void addTransitionListener(@Nonnull StateTransitionListener transitionListener);

  @Nonnull StateType type();

  void stop();
}
