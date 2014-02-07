package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.robotninjas.barge.proto.RaftProto.RequestVote;
import static org.robotninjas.barge.state.RaftStateContext.StateType;

public abstract class BaseState implements State {

  private final StateType type;

  protected BaseState(@Nullable StateType type) {
    this.type = type;
  }

  @Nullable
  @Override
  public StateType type() {
    return type;
  }

  @VisibleForTesting
  boolean shouldVoteFor(@Nonnull RaftLog log, @Nonnull RequestVote request) {

    Optional<Replica> lastVotedFor = log.lastVotedFor();
    Replica candidate = log.getReplica(request.getCandidateId());

    boolean hasAtLeastTerm = request.getLastLogTerm() >= log.lastLogTerm();
    boolean hasAtLeastIndex = request.getLastLogIndex() >= log.lastLogIndex();

    boolean logAsComplete = (hasAtLeastTerm && hasAtLeastIndex);

    boolean alreadyVotedForCandidate = lastVotedFor.equals(Optional.of(candidate));
    boolean notYetVoted = !lastVotedFor.isPresent();

    return (alreadyVotedForCandidate && logAsComplete) || (notYetVoted && logAsComplete) ||
      (request.getLastLogTerm() > log.lastLogTerm()) || (hasAtLeastTerm && (request.getLastLogIndex() > log.lastLogIndex()))
      || logAsComplete;

  }

}
