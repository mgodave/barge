package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.RequestVote;

public abstract class BaseState implements State {

  @VisibleForTesting
  boolean shouldVoteFor(@Nonnull RaftLog log, @Nonnull RequestVote request) {

    Optional<Replica> lastVotedFor = log.lastVotedFor();

    if (!lastVotedFor.isPresent()) {
      return true;
    }

    Replica candidate = Replica.fromString(request.getCandidateId());
    if (lastVotedFor.equals(Optional.of(candidate))) {
      return true;
    }

    if (request.getLastLogTerm() > log.lastLogTerm()) {
      return true;
    }

    if (request.getLastLogTerm() < log.lastLogTerm()) {
      return false;
    }

    return request.getLastLogIndex() >= log.lastLogIndex();

  }

}
