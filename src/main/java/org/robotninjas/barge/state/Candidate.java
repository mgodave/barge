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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftEntry;
import org.robotninjas.barge.rpc.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.util.concurrent.Futures.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.proto.ClientProto.CommitOperation;
import static org.robotninjas.barge.proto.ClientProto.CommitOperationResponse;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Candidate.IsElectedFunction.IsElected;
import static org.robotninjas.barge.state.Candidate.VoteGrantedPredicate.VoteGranted;
import static org.robotninjas.barge.state.Context.StateType.*;

@NotThreadSafe
class Candidate implements State {

  private static final Logger LOGGER = LoggerFactory.getLogger(Candidate.class);
  private static final Random RAND = new Random(System.nanoTime());

  private final RaftLog log;
  private final ScheduledExecutorService scheduler;
  private final long electionTimeout;
  private final Client client;
  private ScheduledFuture<?> electionTimer;
  private ListenableFuture<Boolean> electionResult;

  @Inject
  Candidate(RaftLog log, @RaftScheduler ScheduledExecutorService scheduler,
            @ElectionTimeout long electionTimeout, Client client) {
    this.log = log;
    this.scheduler = scheduler;
    this.electionTimeout = electionTimeout;
    this.client = client;
  }

  @Override
  public void init(@Nonnull final Context ctx) {

    log.term(log.term() + 1);
    log.votedFor(Optional.of(log.self()));

    LOGGER.debug("Election starting for term {}", log.term());

    List<ListenableFuture<RequestVoteResponse>> responses = sendRequests();
    electionResult = transform(successfulAsList(responses), IsElected);

    addCallback(electionResult, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean elected) {
        checkNotNull(elected);
        //noinspection ConstantConditions
        if (elected) {
          transition(ctx, LEADER);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (!electionResult.isCancelled()) {
          LOGGER.debug("Election failed with exception:", t);
        }
      }

    });

    long timeout = electionTimeout + (RAND.nextLong() % electionTimeout);
    electionTimer = scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Election timeout");
        transition(ctx, CANDIDATE);
      }
    }, timeout, MILLISECONDS);

  }

  private void transition(@Nonnull Context ctx, @Nonnull Context.StateType state) {
    ctx.setState(state);
    electionResult.cancel(false);
    electionTimer.cancel(false);
  }

  private void stepDown(@Nonnull Context ctx) {
    transition(ctx, FOLLOWER);
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull Context ctx, @Nonnull RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    Replica candidate = Replica.fromString(request.getCandidateId());

    boolean voteGranted = false;

    if (request.getTerm() > log.term()) {
      log.term(request.getTerm());
      stepDown(ctx);
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

      stepDown(ctx);

      long prevLogIndex = request.getPrevLogIndex();
      long prevLogTerm = request.getPrevLogTerm();
      List<RaftEntry.Entry> entries = request.getEntriesList();
      success = log.append(prevLogIndex, prevLogTerm, entries);

    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(log.term())
      .setSuccess(success)
      .build();

  }

  @Nonnull
  @Override
  public ListenableFuture<CommitOperationResponse> commitOperation(@Nonnull Context ctx, @Nonnull CommitOperation request) throws RaftException {
    throw new NoLeaderException();
  }

  @VisibleForTesting
  List<ListenableFuture<RequestVoteResponse>> sendRequests() {
    List<ListenableFuture<RequestVoteResponse>> responses = Lists.newArrayList();
    for (Replica replica : log.members()) {
      RequestVote request =
        RequestVote.newBuilder()
          .setTerm(log.term())
          .setCandidateId(log.self().toString())
          .setLastLogIndex(log.lastLogIndex())
          .setLastLogTerm(log.lastLogTerm())
          .build();
      responses.add(client.requestVote(replica, request));
    }
    return responses;
  }


  @Immutable
  @VisibleForTesting
  static enum IsElectedFunction implements Function<List<RequestVoteResponse>, Boolean> {

    IsElected;

    @Nullable
    @Override
    public Boolean apply(@Nullable List<RequestVoteResponse> input) {
      checkNotNull(input);
      final int numSent = input.size();
      final int numGranted =
        FluentIterable
          .from(input)
          .filter(notNull())
          .filter(VoteGranted)
          .size();
      return numGranted >= (numSent / 2.0);
    }

  }

  @Immutable
  @VisibleForTesting
  static enum VoteGrantedPredicate implements Predicate<RequestVoteResponse> {

    VoteGranted;

    @Override
    public boolean apply(@Nullable RequestVoteResponse input) {
      checkNotNull(input);
      //noinspection ConstantConditions
      return input.getVoteGranted();
    }

  }

}
