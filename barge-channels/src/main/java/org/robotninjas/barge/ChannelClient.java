/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.robotninjas.barge;

import java.util.concurrent.CompletableFuture;
import org.jetlang.channels.AsyncRequest;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.rpc.RaftClient;

public class ChannelClient implements RaftClient {
    private final ChannelReplica replica;
    private final Fiber fiber;

    ChannelClient(ChannelReplica replica, Fiber fiber) {
        this.replica = replica;
        this.fiber = fiber;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVote request) {
        CompletableFuture<RequestVoteResponse> response = new CompletableFuture<>();
        AsyncRequest.withOneReply(fiber, replica.getRequestVoteChannel(), request, response::complete);
        return response;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntries request) {
        CompletableFuture<AppendEntriesResponse> response = new CompletableFuture<>();
        AsyncRequest.withOneReply(fiber, replica.getAppendEntriesChannel(), request, response::complete);
        return response;
    }
}
