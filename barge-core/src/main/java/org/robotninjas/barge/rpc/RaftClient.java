package org.robotninjas.barge.rpc;

import com.google.common.util.concurrent.ListenableFuture;

import static org.robotninjas.barge.proto.RaftProto.*;

public interface RaftClient {

    ListenableFuture<RequestVoteResponse> requestVote(RequestVote request);

    ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntries request);
}
