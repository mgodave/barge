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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;
import org.robotninjas.barge.proto.RaftEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.proto.ClientProto.CommitOperation;
import static org.robotninjas.barge.proto.ClientProto.CommitOperationResponse;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Context.StateType.CANDIDATE;

@NotThreadSafe
class Follower implements State {

  private static final Logger LOGGER = LoggerFactory.getLogger(Follower.class);

  private final RaftLog log;
  private final ScheduledExecutorService scheduler;
  private final long timeout;
  private final Client client;
  private Optional<Replica> leader = Optional.absent();
  private ScheduledFuture<?> timeoutTask;

  @Inject
  Follower(RaftLog log, @RaftScheduler ScheduledExecutorService scheduler,
           @ElectionTimeout @Nonnegative long timeout, Client client) {

    this.log = checkNotNull(log);
    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout >= 0);
    this.timeout = timeout;
    this.client = checkNotNull(client);

  }

  @Override
  public void init(@Nonnull Context ctx) {
    resetTimeout(ctx);
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull Context ctx, @Nonnull RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() >= log.term()) {

      if (request.getTerm() > log.term()) {
        log.term(request.getTerm());
      }

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = Voting.shouldVoteFor(log, request);

      if (voteGranted) {
        log.votedFor(Optional.of(candidate));
      }

    }

    return RequestVoteResponse.newBuilder()
      .setTerm(log.term())
      .setVoteGranted(voteGranted)
      .build();

  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull Context ctx, @Nonnull AppendEntries request) {

    LOGGER.debug("AppendEntries received for term {}", request.getTerm());

    boolean success = false;

    if (request.getTerm() >= log.term()) {

      if (request.getTerm() > log.term()) {
        log.term(request.getTerm());
      }

      leader = Optional.of(Replica.fromString(request.getLeaderId()));

      resetTimeout(ctx);

      long prevLogIndex = request.getPrevLogIndex();
      long prevLogTerm = request.getPrevLogTerm();
      List<RaftEntry.Entry> entries = request.getEntriesList();
      success = log.append(prevLogIndex, prevLogTerm, entries);

      if (request.getCommitIndex() > log.commitIndex()) {
        log.commitIndex(Math.min(request.getCommitIndex(), log.lastLogIndex()));
      }

    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(log.term())
      .setSuccess(success)
      .build();

  }

  @Nonnull
  @Override
  public ListenableFuture<CommitOperationResponse> commitOperation(@Nonnull Context ctx, @Nonnull CommitOperation request) throws RaftException {

    if (!leader.isPresent()) {
      throw new NoLeaderException();
    }

    return client.commitOperation(leader.get(), request);

  }

  void resetTimeout(@Nonnull final Context ctx) {

    if (null != timeoutTask) {
      timeoutTask.cancel(false);
    }

    timeoutTask = scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        ctx.setState(CANDIDATE);
      }
    }, timeout * 2, MILLISECONDS);

  }

}
