package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.RequestVote;

public abstract class BaseState implements State {

  @VisibleForTesting
  boolean shouldVoteFor(@Nonnull RaftLog log, @Nonnull RequestVote request) {

    if (!log.lastVotedFor().isPresent()) {
      return true;
    }

    if (log.lastVotedFor().equals(Replica.fromString(request.getCandidateId()))) {
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
