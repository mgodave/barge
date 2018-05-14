package org.robotninjas.barge;

import com.google.common.base.Optional;

public class NotLeaderException extends RaftException {

  private final Optional<Replica> leader;

  public NotLeaderException(Optional<Replica> leader) {
    this.leader = leader;
  }

  public Optional<Replica> getLeader() {
    return leader;
  }
}
