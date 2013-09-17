package org.robotninjas.barge.state;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.context.RaftContext;
import org.robotninjas.barge.rpc.RaftProto;

class Voting {

  static boolean shouldVoteFor(RaftContext rctx, RaftProto.RequestVote request) {

    if (!rctx.votedFor().isPresent()) {
      return true;
    }

    if (rctx.votedFor().equals(Replica.fromString(request.getCandidateId()))) {
      return true;
    }

    if (request.getLastLogTerm() > rctx.log().lastLogTerm()) {
      return true;
    }

    if (request.getLastLogTerm() < rctx.log().lastLogTerm()) {
      return false;
    }

    if (request.getLastLogIndex() > rctx.log().lastLogIndex()) {
      return true;
    }

    return false;

  }

}
