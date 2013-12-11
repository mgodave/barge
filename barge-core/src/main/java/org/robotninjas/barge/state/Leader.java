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

package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.rpc.RaftScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;
import static org.robotninjas.barge.state.RaftPredicates.appendSuccessul;
import static org.robotninjas.barge.state.RaftStateContext.StateType.FOLLOWER;

@NotThreadSafe
class Leader extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private final RaftLog log;
  private final ScheduledExecutorService scheduler;
  private final ListeningExecutorService executor;
  private final long timeout;
  private final Map<Replica, ReplicaManager> managers = Maps.newHashMap();
  private final ReplicaManagerFactory replicaManagerFactory;
  private ScheduledFuture<?> heartbeatTask;
  private final SortedMap<Long, SettableFuture<Object>> requests = Maps.newTreeMap();

  @Inject
  Leader(RaftLog log, @RaftExecutor ListeningExecutorService executor, @RaftScheduler ScheduledExecutorService scheduler,
         @ElectionTimeout @Nonnegative long timeout, ReplicaManagerFactory replicaManagerFactory) {

    this.log = checkNotNull(log);
    this.executor = checkNotNull(executor);
    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout > 0);
    this.timeout = timeout;
    this.replicaManagerFactory = checkNotNull(replicaManagerFactory);
  }

  @Override
  public void init(@Nonnull RaftStateContext ctx) {

    for (Replica replica : log.members()) {
      managers.put(replica, replicaManagerFactory.create(replica));
    }

    sendRequests(ctx);
    resetTimeout(ctx);

  }

  private void stepDown(RaftStateContext ctx) {
    heartbeatTask.cancel(false);
    for (ReplicaManager mgr : managers.values()) {
      mgr.shutdown();
    }
    ctx.setState(FOLLOWER);
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() > log.currentTerm()) {

      log.updateCurrentTerm(request.getTerm());
      stepDown(ctx);

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = shouldVoteFor(log, request);

      if (voteGranted) {
        log.updateVotedFor(Optional.of(candidate));
      }

    }

    return RequestVoteResponse.newBuilder()
      .setTerm(log.currentTerm())
      .setVoteGranted(voteGranted)
      .build();

  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {

    boolean success = false;

    if (request.getTerm() > log.currentTerm()) {
      log.updateCurrentTerm(request.getTerm());
      stepDown(ctx);
      success = log.append(request);
    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(log.currentTerm())
      .setSuccess(success)
      .build();

  }

  void resetTimeout(@Nonnull final RaftStateContext ctx) {

    if (null != heartbeatTask) {
      heartbeatTask.cancel(false);
    }

    heartbeatTask = scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Sending heartbeat");
        sendRequests(ctx);
      }
    }, timeout, timeout, MILLISECONDS);

  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {

    resetTimeout(ctx);
    long index = log.append(operation);
    final SettableFuture<Object> future = SettableFuture.create();
    requests.put(index, future);
    final ListenableFuture<Boolean> sendMessageFuture = commit(ctx);
    sendMessageFuture.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          Boolean sent = sendMessageFuture.get();
          if (sent == Boolean.TRUE) {
            // Okay, updateCommitted will be called and we will handle it there
          } else {
            future.setException(new IOException());
          }
        } catch (Throwable t) {
          future.setException(t);
        }
      }
    }, executor);
    return future;
  }

  /**
   * Find the median value of the list of matchIndex, this value is the committedIndex since, by definition, half of the
   * matchIndex values are greater and half are less than this value. So, at least half of the replicas have stored the
   * median value, this is the definition of committed.
   */
  private void updateCommitted() {

    List<ReplicaManager> sorted = newArrayList(managers.values());
    Collections.sort(sorted, new Comparator<ReplicaManager>() {
      @Override
      public int compare(ReplicaManager o, ReplicaManager o2) {
        return Longs.compare(o.getMatchIndex(), o2.getMatchIndex());
      }
    });

    final int middle = (int) Math.ceil(sorted.size() / 2.0);
    final long committed = sorted.get(middle).getMatchIndex();

    SortedMap<Long, SettableFuture<Object>> entries = requests.headMap(committed + 1);
    
    // We need to make a copy, because updateCommitIndex is async
    final Map<Long, SettableFuture<Object>> snapshot = Maps.newHashMap(entries);
    
    log.updateCommitIndex(committed, snapshot);

    entries.clear();

  }

  private void checkTermOnResponse(RaftStateContext ctx, AppendEntriesResponse response) {

    if (response.getTerm() > log.currentTerm()) {
      log.updateCurrentTerm(response.getTerm());
      stepDown(ctx);
    }

  }

  /**
   * Commit a new entry to the cluster
   *
   * @return a future with the result of the commit operation
   */
  @Nonnull
  @VisibleForTesting
  ListenableFuture<Boolean> commit(RaftStateContext ctx) {
    List<ListenableFuture<AppendEntriesResponse>> responses = sendRequests(ctx);
    return majorityResponse(responses, appendSuccessul());
  }

  /**
   * Notify the {@link ReplicaManager} to send an update the next possible time it can
   *
   * @return futures with the result of the update
   */
  @Nonnull
  @VisibleForTesting
  List<ListenableFuture<AppendEntriesResponse>> sendRequests(final RaftStateContext ctx) {
    List<ListenableFuture<AppendEntriesResponse>> responses = newArrayList();
    for (ReplicaManager replicaManager : managers.values()) {
      ListenableFuture<AppendEntriesResponse> response = replicaManager.requestUpdate();
      responses.add(response);
      Futures.addCallback(response, new FutureCallback<AppendEntriesResponse>() {
        @Override
        public void onSuccess(@Nullable AppendEntriesResponse result) {
          updateCommitted();
          checkTermOnResponse(ctx, result);
        }

        @Override
        public void onFailure(Throwable t) {}

      });

    }
    return responses;
  }

}
