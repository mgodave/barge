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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.robotninjas.barge.Replica;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.robotninjas.barge.proto.RaftProto.*;

@Immutable
public class Client {

  @Nonnull private final ListeningExecutorService raftExecutor;
  @Nonnull private final ListeningExecutorService networkCallExecutor;
  @Nonnull private final RaftClientProvider clientProvider;

  @VisibleForTesting
  Client(RaftClientProvider clientProvider, ListeningExecutorService raftExecutor, ListeningExecutorService networkCallExecutor) {
    this.clientProvider = checkNotNull(clientProvider);
    this.raftExecutor = checkNotNull(raftExecutor);
    this.networkCallExecutor = checkNotNull(networkCallExecutor);
  }

  @Inject
  public Client(@Nonnull RaftClientProvider clientProvider, @RaftExecutor ListeningExecutorService raftExecutor) {
    this(clientProvider, raftExecutor, listeningDecorator(newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Barge-Connect-Thread")
      .build())));
  }

  @Nonnull
  public ListenableFuture<RequestVoteResponse> requestVote(@Nonnull final Replica replica, @Nonnull final RequestVote request) {
    checkNotNull(replica);
    checkNotNull(request);

    // Put a (possibly) blocking connect onto a different thread
    ListenableFuture<ListenableFuture<RequestVoteResponse>> response =
      networkCallExecutor.submit(new Callable<ListenableFuture<RequestVoteResponse>>() {
        @Override
        public ListenableFuture<RequestVoteResponse> call() throws Exception {
          RaftClient client = clientProvider.get(replica);
          return client.requestVote(request);
        }
      });

    // Transfer the response back onto the raft thread
    return transform(response, Identity.<RequestVoteResponse>identity(), raftExecutor);

  }

  @Nonnull
  public ListenableFuture<AppendEntriesResponse> appendEntries(@Nonnull final Replica replica, @Nonnull final AppendEntries request) {
    checkNotNull(replica);
    checkNotNull(request);

    // Put a (possibly) blocking connect onto a different thread
    ListenableFuture<ListenableFuture<AppendEntriesResponse>> response =
      networkCallExecutor.submit(new Callable<ListenableFuture<AppendEntriesResponse>>() {
        @Override
        public ListenableFuture<AppendEntriesResponse> call() throws Exception {
          RaftClient client = clientProvider.get(replica);
          return client.appendEntries(request);
        }
      });

    // Transfer the response back onto the raft thread
    return transform(response, Identity.<AppendEntriesResponse>identity(), raftExecutor);

  }

  private static final class Identity<E> implements AsyncFunction<ListenableFuture<E>, E> {

    Identity() {}

    @Override
    public ListenableFuture<E> apply(ListenableFuture<E> input) throws Exception {
      return input;
    }

    public static <T> Identity<T> identity() {
      return new Identity<T>();
    }
  }

}
