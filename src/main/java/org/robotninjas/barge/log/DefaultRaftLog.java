package org.robotninjas.barge.log;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.LocalReplicaInfo;
import org.robotninjas.barge.rpc.RaftProto;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class DefaultRaftLog implements RaftLog {

  private final Replica local;

  @Inject
  public DefaultRaftLog(@LocalReplicaInfo Replica local) {
    this.local = local;
  }

  @Override
  public boolean append(RaftProto.AppendEntries request) {
    return false;
  }

  @Override
  public List<Replica> members() {
    Replica replica1 = Replica.fromString("localhost:10000");
    Replica replica2 = Replica.fromString("localhost:10001");
    Replica replica3 = Replica.fromString("localhost:10002");
    List<Replica> replicas = newArrayList(replica1, replica2, replica3);
    replicas.remove(local);
    return replicas;
  }

  @Override
  public long lastLogIndex() {
    return 0;
  }

  @Override
  public long lastLogTerm() {
    return 0;
  }
}
