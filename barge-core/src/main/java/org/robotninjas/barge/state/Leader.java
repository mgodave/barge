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
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry.Membership;
import org.robotninjas.barge.rpc.RaftScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.RaftStateContext.StateType.FOLLOWER;

@NotThreadSafe
class Leader extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private final RaftLog log;
  private final ScheduledExecutorService scheduler;
  private final long timeout;
  private final Map<Replica, ReplicaManager> managers = Maps.newHashMap();
  private final ReplicaManagerFactory replicaManagerFactory;
  private ScheduledFuture<?> heartbeatTask;

  @Inject
  Leader(RaftLog log, @RaftScheduler ScheduledExecutorService scheduler,
         @ElectionTimeout @Nonnegative long timeout, ReplicaManagerFactory replicaManagerFactory) {

    this.log = checkNotNull(log);
    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout > 0);
    this.timeout = timeout;
    this.replicaManagerFactory = checkNotNull(replicaManagerFactory);
  }

  @Override
  public void init(@Nonnull RaftStateContext ctx) {

    for (Replica replica : ctx.getConfigurationState().remote()) {
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
    ctx.setState(this, FOLLOWER);
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() > log.currentTerm()) {

      log.currentTerm(request.getTerm());
      stepDown(ctx);

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = shouldVoteFor(log, request);

      if (voteGranted) {
        log.lastVotedFor(Optional.of(candidate));
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
      log.currentTerm(request.getTerm());
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

    ListenableFuture<Object> result = log.append(operation, null);
    sendRequests(ctx);
    return result;

  }

  ListenableFuture<Object> commitMembership(@Nonnull RaftStateContext ctx, @Nonnull Membership membership) {

    resetTimeout(ctx);
    
    ListenableFuture<Object> result = log.append(null, membership);
    sendRequests(ctx);
    return result;

  }

  /**
   * Find the median value of the list of matchIndex, this value is the committedIndex since, by definition, half of the
   * matchIndex values are greater and half are less than this value. So, at least half of the replicas have stored the
   * median value, this is the definition of committed.
   */
  long getQuorumMatchIndex(List<String> serverKeys) {
    List<Long> sorted = newArrayList();
    for (String serverKey : serverKeys) {
      ReplicaManager replicaManager = managers.get(serverKey);
      if (replicaManager != null) {
        sorted.add(replicaManager.getMatchIndex());
      } else if (serverKey.equals(log.self().getKey())) {
        sorted.add(log.lastLogIndex());
      } else {
        throw new IllegalStateException("No replica manager for server: " + serverKey);
      }
    }
    Collections.sort(sorted);

    int n = sorted.size();
    int quorumSize;
    if ((n & 1) == 1) {
      // Odd
      quorumSize = (n + 1) / 2;
    } else {
      // Even
      quorumSize = (n / 2) + 1;
    }
    final int middle = quorumSize - 1;
    final long committed = sorted.get(middle);
    
    return committed;
  }

  long getQuorumMatchIndex(ConfigurationState configurationState) {
    if (configurationState.isTransitional()) {
      return Math.min(
          getQuorumMatchIndex(configurationState.getMembership().getMembersList()),
          getQuorumMatchIndex(configurationState.getMembership().getProposedMembersList()));      
    } else {
      return getQuorumMatchIndex(configurationState.getMembership().getMembersList());
    }
  }

  private void updateCommitted(RaftStateContext ctx) {
    ConfigurationState configurationState = ctx.getConfigurationState();

    long committed = getQuorumMatchIndex(configurationState);
    log.commitIndex(committed);

    if (committed >= configurationState.getId()) {
      handleConfigurationUpdate(ctx);
    }
 
  }

  private void handleConfigurationUpdate(RaftStateContext ctx) {
    ConfigurationState configurationState = ctx.getConfigurationState();
    
    // Upon committing a configuration that excludes itself, the leader
    // steps down.
    if (!configurationState.hasVote(configurationState.self())) {
        stepDown(ctx /*logcabin: currentTerm + 1*/);
        return;
    }

    // Upon committing a reconfiguration (Cold,new) entry, the leader
    // creates the next configuration (Cnew) entry.
    if (configurationState.isTransitional()) {
      final Membership membership = configurationState.getMembership();
      
      Membership.Builder members = Membership.newBuilder();
      members.addAllMembers(membership.getProposedMembersList());
      
      Futures.addCallback(commitMembership(ctx, members.build()),
          new FutureCallback<Object>() {

            @Override
            public void onSuccess(Object result) {
              if (Boolean.TRUE.equals(result)) {
                LOGGER.info("Committed new cluster configuration: {}", membership);
              } else {
                LOGGER.warn("Failed to commit new cluster configuration: {}", membership);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              LOGGER.warn("Error committing new cluster configuration: {}", membership);
            }
          });
      }
  }

  private void checkTermOnResponse(RaftStateContext ctx, AppendEntriesResponse response) {

      if (response.getTerm() > log.currentTerm()) {
        log.currentTerm(response.getTerm());
        stepDown(ctx);
      }

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
          updateCommitted(ctx);
          checkTermOnResponse(ctx, result);
        }

        @Override
        public void onFailure(Throwable t) {}

      });

    }
    
    if (responses.isEmpty()) {
      updateCommitted(ctx);
    }
    return responses;
  }
  
  @Override
  public ListenableFuture<Boolean> setConfiguration(@Nonnull RaftStateContext ctx, long oldId, final Membership nextConfiguration) throws RaftException {
    if (nextConfiguration.getProposedMembersCount() != 0) {
      // We expect members to be passed in, not proposed_members
      throw new IllegalArgumentException();
    }
    
    final ConfigurationState configuration = ctx.getConfigurationState();
    if (configuration.getId() != oldId || configuration.isTransitional()) {
      // configurations has changed in the meantime
      return Futures.immediateFuture(Boolean.FALSE);
    }

    // TODO: Introduce Staging state; wait for staging servers to catch up

    // Commit a transitional configuration with old and new
    Membership.Builder transitionalMembership = Membership.newBuilder();
    transitionalMembership.addAllMembers(configuration.getMembership().getMembersList());
    transitionalMembership.addAllProposedMembers(nextConfiguration.getMembersList());
    ListenableFuture<Object> transitionFuture = commitMembership(ctx, transitionalMembership.build());
    
    return Futures.transform(transitionFuture, new AsyncFunction<Object, Boolean>() {
      // In response to the transitional configuration, the leader commits the final configuration
      // We poll for this configuration
      @Override
      public ListenableFuture<Boolean> apply(Object input) throws Exception {
        if (!Boolean.TRUE.equals(input)) {
          return Futures.immediateFuture(Boolean.FALSE);
        }
        
        return new PollFor<Boolean>(scheduler, 200, 200, TimeUnit.MILLISECONDS) {
          @Override
          protected Optional<Boolean> poll() {
            if (!configuration.isTransitional()) {
              return Optional.of(configuration.getMembership().equals(nextConfiguration));
            }
            return Optional.absent();
          }
        }.start();
      }
    });
  }

}
