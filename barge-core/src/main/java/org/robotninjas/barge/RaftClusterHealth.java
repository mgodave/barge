package org.robotninjas.barge;

import java.util.List;

public class RaftClusterHealth {

  final RaftMembership membership;
  final List<Replica> deadPeers;

  public RaftClusterHealth(RaftMembership membership, List<Replica> deadPeers) {
    this.membership = membership;
    this.deadPeers = deadPeers;
  }

  public RaftMembership getClusterMembership() {
    return membership;
  }

  public List<Replica> getDeadPeers() {
    return deadPeers;
  }

}
