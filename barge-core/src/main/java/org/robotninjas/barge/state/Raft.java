package org.robotninjas.barge.state;

import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

import javax.annotation.Nonnull;


/**
 * Main interface to a Raft protocol instance.
 */
public interface Raft {

  public static enum StateType {
    START, FOLLOWER, CANDIDATE, LEADER, STOPPED
  }

  void addRaftProtocolListener(RaftProtocolListener protocolListener);

  ListenableFuture<StateType> init();

  @Nonnull RequestVoteResponse requestVote(@Nonnull RequestVote request);

  @Nonnull AppendEntriesResponse appendEntries(@Nonnull AppendEntries request);

  @Nonnull ListenableFuture<Object> commitOperation(@Nonnull byte[] op) throws RaftException;

  void addTransitionListener(@Nonnull StateTransitionListener transitionListener);

  @Nonnull StateType type();

  void stop();
}
