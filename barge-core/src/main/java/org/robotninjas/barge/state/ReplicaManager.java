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
import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import com.google.inject.assistedinject.Assisted;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import javax.inject.Inject;


/**
 * Manages a single {@link org.robotninjas.barge.Replica} instance on behalf of a {@link org.robotninjas.barge.state.Leader}.
 *
 * TODO implement optimization in section 5.3
 */
@NotThreadSafe
class ReplicaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaManager.class);
  private static final int BATCH_SIZE = 1000;

  private final Client client;
  private final RaftLog log;
  private final Replica remote;
  private long nextIndex;
  private long matchIndex = 0;
  private boolean requested = false;
  private boolean waitingForResponse = false;
  private boolean forwards = false;
  private boolean shutdown = false;
  private SettableFuture<AppendEntriesResponse> nextResponse = SettableFuture.create();

  @Inject
  ReplicaManager(Client client, RaftLog log, @Assisted Replica remote) {

    this.nextIndex = log.lastLogIndex() + 1;
    this.log = log;
    this.client = client;
    this.remote = remote;

  }

  private ListenableFuture<AppendEntriesResponse> sendUpdate() {

    waitingForResponse = true;
    requested = false;

    // if the last rpc call was successful then try to send
    // as many updates as we can, otherwise, if we're
    // probing backwards, only send one entry as a probe, as
    // soon as we have a successful call forwards will become
    // true and we can catch up quickly
    GetEntriesResult result = log.getEntriesFrom(nextIndex, forwards ? BATCH_SIZE : 1);

    final AppendEntries request = AppendEntries.newBuilder()
        .setTerm(log.currentTerm())
        .setLeaderId(log.self().toString())
        .setPrevLogIndex(result.lastLogIndex())
        .setPrevLogTerm(result.lastLogTerm())
        .setCommitIndex(log.commitIndex())
        .addAllEntries(result.entries())
        .build();

    LOGGER.debug("Sending update to {} prevLogIndex: {}, prevLogTerm: {}, nr. of entries {}", remote, result.lastLogIndex(),
      result.lastLogTerm(), result.entries().size());

    final ListenableFuture<AppendEntriesResponse> response = client.appendEntries(remote, request);

    final SettableFuture<AppendEntriesResponse> previousResponse = nextResponse;

    Futures.addCallback(response, new FutureCallback<AppendEntriesResponse>() {

        @Override
        public void onSuccess(@Nullable AppendEntriesResponse result) {
          waitingForResponse = false;

          if (result != null) {
            updateNextIndex(request, result);

            if (result.getSuccess()) {
              previousResponse.set(result);
            }
          }

        }

        @Override
        public void onFailure(@Nonnull Throwable t) {
          waitingForResponse = false;
          requested = false;
          previousResponse.setException(t);
        }

      });

    nextResponse = SettableFuture.create();

    return response;

  }

  private void updateNextIndex(@Nonnull AppendEntries request, @Nonnull AppendEntriesResponse response) {

    boolean runAgain = requested || !response.getSuccess();
    forwards = response.getSuccess();
    requested = false;

    LOGGER.debug("Response from {} Status {} nextIndex {}, matchIndex {}", remote, response.getSuccess(), nextIndex, matchIndex);

    if (response.getSuccess()) {
      nextIndex = Math.max(nextIndex, request.getPrevLogIndex() + request.getEntriesCount() + 1);
      matchIndex = Math.max(matchIndex, request.getPrevLogIndex() + request.getEntriesCount());
    } else {
      nextIndex = Math.max(1, nextIndex - 1);
    }

    if (runAgain && !shutdown) {
      LOGGER.debug("continue sending update");
      sendUpdate();
    }

    LOGGER.debug("Response from {} Status {} nextIndex {}, matchIndex {}", remote, response.getSuccess(), nextIndex, matchIndex);

  }

  @VisibleForTesting
  boolean isRunning() {
    return waitingForResponse;
  }

  @VisibleForTesting
  boolean isRequested() {
    return requested;
  }

  @VisibleForTesting
  long getNextIndex() {
    return nextIndex;
  }

  @VisibleForTesting
  long getMatchIndex() {
    return matchIndex;
  }

  public void shutdown() {
    shutdown = true;
  }

  @Nonnull
  public ListenableFuture<AppendEntriesResponse> requestUpdate() {
    requested = true;

    if (!waitingForResponse) {
      return sendUpdate();
    }

    return nextResponse;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass()).add("nextIndex", nextIndex).add("matchIndex", matchIndex).toString();
  }
}
