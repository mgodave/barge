package org.robotninjas.barge;

public class NotLeaderException extends RaftException {

  private final Replica leader;

  public NotLeaderException(Replica leader) {
    this.leader = leader;
  }

  public Replica getLeader() {
    return leader;
  }
}
