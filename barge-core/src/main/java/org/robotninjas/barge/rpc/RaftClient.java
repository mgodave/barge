package org.robotninjas.barge.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

public interface RaftClient {

    ListenableFuture<RequestVoteResponse> requestVote(RequestVote request);

    ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntries request);
}
