package org.robotninjas.barge.rpc;

import java.util.concurrent.CompletableFuture;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

public interface RaftClient {

    CompletableFuture<RequestVoteResponse> requestVote(RequestVote request);

    CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntries request);
}
