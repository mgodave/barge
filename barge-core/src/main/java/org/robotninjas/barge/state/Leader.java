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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;
import static org.robotninjas.barge.state.Raft.StateType.LEADER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.NoLongerLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@NotThreadSafe
class Leader extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private final Fiber scheduler;
  private final long timeout;
  private final Map<Replica, ReplicaManager> managers = Maps.newHashMap();
  private final ReplicaManagerFactory replicaManagerFactory;
  private final List<CompletableFuture<Object>> outstanding = synchronizedList(newLinkedList());
  private Disposable heartbeatTask;

  @Inject
  Leader(RaftLog log, @RaftExecutor Fiber scheduler, @ElectionTimeout @Nonnegative long timeout,
    ReplicaManagerFactory replicaManagerFactory) {

    super(LEADER, log);

    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout > 0);
    this.timeout = timeout;
    this.replicaManagerFactory = checkNotNull(replicaManagerFactory);

  }

  @Override
  public void init(@Nonnull RaftStateContext ctx) {

    for (Replica replica : getLog().members()) {
      managers.put(replica, replicaManagerFactory.create(replica));
    }

    sendRequests(ctx);
    resetTimeout(ctx);

  }

  @Override
  public void doStop(RaftStateContext ctx) {
    destroy(ctx);
    super.doStop(ctx);
  }

  public void destroy(RaftStateContext ctx) {
    if (null != heartbeatTask) {
      heartbeatTask.dispose();
    }

    for (ReplicaManager mgr : managers.values()) {
      mgr.shutdown();
    }

    outstanding.forEach(o ->
        o.cancel(true)
    );
  }

  void resetTimeout(@Nonnull final RaftStateContext ctx) {

    if (null != heartbeatTask) {
      heartbeatTask.dispose();
    }

    heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
      LOGGER.debug("Sending heartbeat");
      sendRequests(ctx);
    }, timeout, timeout, MILLISECONDS);

  }

  @Nonnull
  @Override
  public CompletableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    resetTimeout(ctx);

    CompletableFuture<Object> result = getLog().append(operation);
    sendRequests(ctx);

    outstanding.add(result);

    result.handle((r, e) -> {
      outstanding.remove(result);
      return null;
    });

    return result;

  }

  /**
   * Find the median value of the list of matchIndex, this value is the committedIndex since, by definition, half of the
   * matchIndex values are greater and half are less than this value. So, at least half of the replicas have stored the
   * median value, this is the definition of committed.
   */
  private void updateCommitted() {

    List<Long> sorted = newArrayList();

    for (ReplicaManager manager : managers.values()) {
      sorted.add(manager.getMatchIndex());
    }

    sorted.add(getLog().lastLogIndex());
    Collections.sort(sorted);

    int n = sorted.size();
    int quorumSize = (n / 2) + 1;
    final long committed = sorted.get(quorumSize - 1);

    LOGGER.debug("updating commitIndex to {}; sorted is {}", committed, sorted);
    getLog().commitIndex(committed);

  }

  private void checkTermOnResponse(RaftStateContext ctx, AppendEntriesResponse response) {

    if ((response != null) && (response.getTerm() > getLog().currentTerm())) {
      getLog().currentTerm(response.getTerm());
      ctx.setState(this, FOLLOWER);
    }

  }

  /**
   * Notify the {@link ReplicaManager} to send an update the next possible time it can
   *
   * @return futures with the result of the update
   */
  @Nonnull
  @VisibleForTesting
  List<CompletableFuture<AppendEntriesResponse>> sendRequests(final RaftStateContext ctx) {

    List<CompletableFuture<AppendEntriesResponse>> responses =
        managers.values().stream()
            .map(ReplicaManager::requestUpdate)
            .collect(toList());

    responses.forEach(response ->
        response.thenAccept(result -> {
          updateCommitted();
          checkTermOnResponse(ctx, result);
        }));


    // Cope if we're the only node in the cluster.
    if (responses.isEmpty()) {
      updateCommitted();
    }

    return responses;
  }

}
