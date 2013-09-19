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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.barge.context.RaftContext;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RpcClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.robotninjas.barge.rpc.RaftProto.*;
import static org.robotninjas.barge.state.Context.StateType.FOLLOWER;

public class Leader implements State {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private final RaftContext rctx;
  private final ScheduledExecutorService scheduler;
  private final long timeout;
  private final RpcClientProvider clientProvider;
  private final Map<Replica, ReplicaManager> managers = Maps.newHashMap();

  @Inject
  Leader(RaftContext rctx, @RaftScheduler ScheduledExecutorService scheduler,
         @ElectionTimeout long timeout, RpcClientProvider clientProvider) {

    this.rctx = rctx;
    this.scheduler = scheduler;
    this.timeout = timeout;
    this.clientProvider = clientProvider;
  }

  @Override
  public void init(Context ctx) {

    RaftLog log = rctx.log();
    long nextIndex = log.lastLogIndex() + 1;
    long term = rctx.term();
    Replica self = rctx.self();

    for (Replica replica : rctx.log().members()) {
      RaftClient client = clientProvider.get(replica);
      managers.put(replica, new ReplicaManager(client, log, term, nextIndex, self));
    }

    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Sending heartbeat");
        sendRequests();
      }
    }, 0, timeout, TimeUnit.MILLISECONDS);

  }

  @Override
  public RequestVoteResponse requestVote(Context ctx, RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() > rctx.term()) {

      rctx.term(request.getTerm());
      ctx.setState(FOLLOWER);

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = Voting.shouldVoteFor(rctx, request);

      if (voteGranted) {
        rctx.votedFor(Optional.of(candidate));
      }

    }

    return RequestVoteResponse.newBuilder()
      .setTerm(rctx.term())
      .setVoteGranted(voteGranted)
      .build();

  }

  @Override
  public AppendEntriesResponse appendEntries(Context ctx, AppendEntries request) {

    boolean success = false;

    if (request.getTerm() > rctx.term()) {
      rctx.term(request.getTerm());
      ctx.setState(FOLLOWER);
      success = rctx.log().append(request);
    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(rctx.term())
      .setSuccess(success)
      .build();

  }

  @VisibleForTesting
  List<ListenableFuture<AppendEntriesResponse>> sendRequests() {
    List<ListenableFuture<AppendEntriesResponse>> responses = Lists.newArrayList();
    for (ReplicaManager replicaManager : managers.values()) {
      responses.add(replicaManager.fireUpdate());
    }
    return responses;
  }

  static class ReplicaManager {

    private final RaftClient client;
    private final RaftLog log;
    private final long term;
    private final Replica self;
    private long nextIndex;
    private boolean running = false;
    private boolean requested = false;
    private SettableFuture<AppendEntriesResponse> returnable = SettableFuture.create();

    ReplicaManager(RaftClient client, RaftLog log, long term, long nextIndex, Replica self) {
      this.nextIndex = nextIndex;
      this.term = term;
      this.log = log;
      this.client = client;
      this.self = self;
    }

    @VisibleForTesting
    void sendUpdate() {

      LOGGER.debug("Sending update");

      running = true;

      GetEntriesResult result =
        log.getEntriesFrom(nextIndex);

      final AppendEntries request =
        AppendEntries.newBuilder()
          .setTerm(term)
          .setLeaderId(self.toString())
          .setPrevLogIndex(result.lastLogIndex())
          .setPrevLogTerm(result.lastLogTerm())
          .setCommitIndex(log.commitIndex())
          .addAllEntries(result.entries())
          .build();

      final ListenableFuture<AppendEntriesResponse> response =
        client.appendEntries(request);

      response.addListener(new Runnable() {
        @Override
        public void run() {
          update(request, response, returnable);
        }
      }, sameThreadExecutor());

      returnable = SettableFuture.create();

    }

    @VisibleForTesting
    void update(AppendEntries request, ListenableFuture<AppendEntriesResponse> f,
                SettableFuture<AppendEntriesResponse> returnable) {

      running = requested;
      requested = false;

      try {

        AppendEntriesResponse response = f.get();

        running = running || !response.getSuccess();

        if (response.getSuccess()) {
          nextIndex += request.getEntriesCount();
          returnable.set(response);
        } else {
          nextIndex -= 1;
        }

        if (running) {
          sendUpdate();
        }

      } catch (InterruptedException e) {

        Throwables.propagate(e);

      } catch (ExecutionException e) {

        returnable.setException(e);

      }

    }

    @VisibleForTesting
    boolean isRunning() {
      return running;
    }

    @VisibleForTesting
    boolean isRequested() {
      return requested;
    }

    @VisibleForTesting
    long getNextIndex() {
      return nextIndex;
    }

    public ListenableFuture<AppendEntriesResponse> fireUpdate() {
      if (!running) {
        running = true;
        sendUpdate();
      } else {
        requested = true;
      }
      return returnable;
    }

  }


}
