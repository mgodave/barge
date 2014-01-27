package org.robotninjas.barge.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.proto.RaftProto;

public interface AsynchronousRaftClient {
    ListenableFuture<RaftProto.RequestVoteResponse> requestVote(RaftProto.RequestVote request);

    ListenableFuture<RaftProto.AppendEntriesResponse> appendEntries(RaftProto.AppendEntries request);
}
