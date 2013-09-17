package org.robotninjas.barge.log;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.rpc.RaftProto;

import java.util.List;

public interface RaftLog {

  boolean append(RaftProto.AppendEntries request);

  List<Replica> members();

  long lastLogIndex();

  long lastLogTerm();

}
