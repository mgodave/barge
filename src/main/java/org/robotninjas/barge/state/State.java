package org.robotninjas.barge.state;

import static org.robotninjas.barge.rpc.RaftProto.*;

interface State {

  void init(Context ctx);

  RequestVoteResponse requestVote(Context ctx, RequestVote request);

  AppendEntriesResponse appendEntries(Context ctx, AppendEntries request);

}
