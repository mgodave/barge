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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.assistedinject.Assisted;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.AppendEntriesResponse;


/**
 * TODO implement optimization in section 5.3
 */
@NotThreadSafe
class ReplicaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaManager.class);
  private static final int BATCH_SIZE = 200;

  private final Client client;
  private final RaftLog log;
  private final Replica remote;
  private long nextIndex;
  private long matchIndex = 0;
  private boolean running = false;
  private boolean requested = false;
  private boolean forwards = false;
  private SettableFuture<AppendEntriesResponse> nextResponse = SettableFuture.create();

  @Inject
  ReplicaManager(Client client, RaftLog log, @Assisted Replica remote) {

    this.nextIndex = log.lastLogIndex() + 1;
    this.log = log;
    this.client = client;
    this.remote = remote;

  }

  private ListenableFuture<AppendEntriesResponse> sendUpdate() {

    running = true;
    requested = false;

    // if the last rpc call was successful then try to send
    // as many updates as we can, otherwise, if we're
    // probing backwards, only send one entry as a probe, as
    // soon as we have a successful call forwards will become
    // true and we can catch up quickly
    GetEntriesResult result = log.getEntriesFrom(nextIndex, forwards ? BATCH_SIZE : 1);

    final AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(log.currentTerm())
        .setLeaderId(log.self().toString())
        .setPrevLogIndex(result.lastLogIndex())
        .setPrevLogTerm(result.lastLogTerm())
        .setCommitIndex(log.commitIndex())
        .addAllEntries(result.entries())
        .build();

    final ListenableFuture<AppendEntriesResponse> response = client.appendEntries(remote, request);

    final SettableFuture<AppendEntriesResponse> previousResponse = nextResponse;

    Futures.addCallback(response, new FutureCallback<AppendEntriesResponse>() {

      @Override
      public void onSuccess(@Nullable AppendEntriesResponse result) {
        updateNextIndex(request, result);
        if (result.getSuccess()) {
          previousResponse.set(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        running = requested;
        requested = false;
        previousResponse.setException(t);
      }

    });

    nextResponse = SettableFuture.create();

    return response;

  }

  private void updateNextIndex(@Nonnull AppendEntries request, @Nonnull AppendEntriesResponse response) {

    running = requested || !response.getSuccess();
    forwards = response.getSuccess();
    requested = false;

    if (response.getSuccess()) {
      nextIndex += request.getEntriesCount();
      matchIndex = request.getPrevLogIndex() + request.getEntriesCount();
    } else {
      nextIndex = Math.max(1, nextIndex - 1);
    }

    if (running) {
      sendUpdate();
    }

  }

  public Replica getRemote() {
    return remote;
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

  @VisibleForTesting
  long getMatchIndex() {
    return matchIndex;
  }

  public void shutdown() {

  }

  @Nonnull
  public ListenableFuture<AppendEntriesResponse> requestUpdate() {
    requested = true;
    if (!running) {
      return sendUpdate();
    }
    return nextResponse;
  }

}

