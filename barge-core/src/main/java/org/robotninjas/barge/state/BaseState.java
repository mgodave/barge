package org.robotninjas.barge.state;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;

import javax.annotation.Nonnull;

public abstract class BaseState implements State {

  protected boolean shouldVoteFor(@Nonnull RaftLog log, @Nonnull RaftProto.RequestVote request) {

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
