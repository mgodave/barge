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
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.RaftScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;

@NotThreadSafe
class Follower extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Follower.class);

  private final RaftLog log;
  private final ScheduledExecutorService scheduler;
  private final long timeout;
  private Optional<Replica> leader = Optional.absent();
  private DeadlineTimer timeoutTask;

  @Inject
  Follower(RaftLog log, @RaftScheduler ScheduledExecutorService scheduler, @ElectionTimeout @Nonnegative long timeout) {
    super(FOLLOWER);

    this.log = checkNotNull(log);
    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout >= 0);
    this.timeout = timeout;

  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {
    timeoutTask = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("DeadlineTimer expired, starting election");
        ctx.setState(Follower.this, CANDIDATE);
      }
    }, timeout * 2);
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() >= log.currentTerm()) {

      if (request.getTerm() > log.currentTerm()) {
        log.currentTerm(request.getTerm());
      }

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

    LOGGER.debug("AppendEntries prev index {}, prev term {}, num entries {}, term {}",
      request.getPrevLogIndex(), request.getPrevLogTerm(), request.getEntriesCount(), request.getTerm());

    boolean success = false;

    if (request.getTerm() >= log.currentTerm()) {

      if (request.getTerm() > log.currentTerm()) {
        log.currentTerm(request.getTerm());
      }

      leader = Optional.of(Replica.fromString(request.getLeaderId()));
      timeoutTask.reset();
      success = log.append(request);

      if (request.getCommitIndex() > log.commitIndex()) {
        log.commitIndex(Math.min(request.getCommitIndex(), log.lastLogIndex()));
      }

    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(log.currentTerm())
      .setSuccess(success)
      .setLastLogIndex(log.lastLogIndex())
      .build();

  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    if (leader.isPresent()) {
      throw new NotLeaderException(leader.get());
    } else {
      throw new NoLeaderException();
    }
  }

}
