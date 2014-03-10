/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.robotninjas.barge.rpc;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class Client {

  @Nonnull
  private final RaftClientProvider clientProvider;
  @Nonnull
  private final Executor executor;

  @Inject
  public Client(@Nonnull RaftClientProvider clientProvider, @Nonnull @RaftExecutor Fiber executor) {
    this.clientProvider = clientProvider;
    this.executor = executor;
  }

  @Nonnull
  public ListenableFuture<RequestVoteResponse> requestVote(@Nonnull final Replica replica, @Nonnull final RequestVote request) {

    checkNotNull(replica);
    checkNotNull(request);

    ListenableFuture<RequestVoteResponse> response = clientProvider.get(replica).requestVote(request);
    return Futures.transform(response, Functions.<RequestVoteResponse>identity(), executor);
  }

  @Nonnull
  public ListenableFuture<AppendEntriesResponse> appendEntries(@Nonnull final Replica replica, @Nonnull final AppendEntries request) {

    checkNotNull(replica);
    checkNotNull(request);

    ListenableFuture<AppendEntriesResponse> response = clientProvider.get(replica).appendEntries(request);
    return Futures.transform(response, Functions.<AppendEntriesResponse>identity(), executor);
  }

}
