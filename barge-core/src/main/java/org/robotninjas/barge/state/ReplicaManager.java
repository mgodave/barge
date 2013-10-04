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
import com.google.common.base.Throwables;
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
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.AppendEntriesResponse;


/**
 * TODO implement optimization in section 5.3
 */
@NotThreadSafe
class ReplicaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaManager.class);
  private static final int BATCH_SIZE = 1000;

  private final Client client;
  private final RaftLog log;
  private final long term;
  private final Replica self;
  private final Replica remote;
  private long nextIndex;
  private boolean running = false;
  private boolean requested = false;
  private boolean forwards = false;
  private SettableFuture<AppendEntriesResponse> returnable = SettableFuture.create();

  @SuppressWarnings("ConstructorWithTooManyParameters")
  @Inject
  ReplicaManager(Client client, RaftLog log, @Assisted("term") long term,
                 @Assisted("nextIndex") long nextIndex, @Assisted("remote") Replica remote,
                 @Assisted("self") Replica self) {

    this.nextIndex = nextIndex;
    this.term = term;
    this.log = log;
    this.client = client;
    this.remote = remote;
    this.self = self;

  }

  @VisibleForTesting
  void sendUpdate() {

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
        .setTerm(term)
        .setLeaderId(self.toString())
        .setPrevLogIndex(result.lastLogIndex())
        .setPrevLogTerm(result.lastLogTerm())
        .setCommitIndex(log.commitIndex())
        .addAllEntries(result.entries())
        .build();

    final ListenableFuture<AppendEntriesResponse> response = client.appendEntries(remote, request);

    response.addListener(new Runnable() {
      @Override
      public void run() {
        update(request, response, returnable);
      }
    }, sameThreadExecutor());

    returnable = SettableFuture.create();

  }

  @VisibleForTesting
  void update(@Nonnull AppendEntries request, @Nonnull ListenableFuture<AppendEntriesResponse> f,
              @Nonnull SettableFuture<AppendEntriesResponse> returnable) {

    running = requested;
    requested = false;

    try {

      AppendEntriesResponse response = f.get();

      running = running || !response.getSuccess();
      forwards = response.getSuccess();

      if (response.getSuccess()) {
        nextIndex += request.getEntriesCount();
        returnable.set(response);
      } else if (response.hasLastLogIndex()) {
        nextIndex = response.getLastLogIndex() + 1;
      } else {
        nextIndex = nextIndex == 0 ? 0 : nextIndex - 1;
      }

      if (running) {
        sendUpdate();
      }

    } catch (InterruptedException e) {

      throw Throwables.propagate(e);

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

  @Nonnull
  public ListenableFuture<AppendEntriesResponse> fireUpdate() {
    requested = true;
    if (!running) {
      sendUpdate();
    }
    return returnable;
  }

}

