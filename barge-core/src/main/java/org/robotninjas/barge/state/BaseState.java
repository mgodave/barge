package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.RaftStateContext.StateType.*;

public abstract class BaseState<T> implements State<T> {

  private final RaftLog log;
  private Optional<Replica> leader;

  protected BaseState(RaftLog log) {
    this.log = log;
  }

  protected RaftLog getLog() {
    return log;
  }

  protected Optional<Replica> getLeader() {
    return leader;
  }

  @Override
  public void destroy(RaftStateContext ctx) {

  }

  @VisibleForTesting
  boolean shouldVoteFor(@Nonnull RaftLog log, @Nonnull RequestVote request) {

    Optional<Replica> lastVotedFor = log.lastVotedFor();
    Replica candidate = Replica.fromString(request.getCandidateId());

    boolean hasAtLeastTerm = request.getLastLogTerm() >= log.lastLogTerm();
    boolean hasAtLeastIndex = request.getLastLogIndex() >= log.lastLogIndex();

    boolean logAsComplete = (hasAtLeastTerm && hasAtLeastIndex);

    boolean alreadyVotedForCandidate = lastVotedFor.equals(Optional.of(candidate));
    boolean notYetVoted = !lastVotedFor.isPresent();

    return (alreadyVotedForCandidate && logAsComplete) || (notYetVoted && logAsComplete) ||
      (request.getLastLogTerm() > log.lastLogTerm()) || (hasAtLeastTerm && (request.getLastLogIndex() > log.lastLogIndex()))
      || logAsComplete;

  }

  protected void resetTimer() {

  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {

    boolean success = false;

    if (request.getTerm() >= log.currentTerm()) {

      if (request.getTerm() > log.currentTerm()) {

        log.currentTerm(request.getTerm());

        if (ctx.getState().equals(LEADER) || ctx.getState().equals(CANDIDATE)) {
          ctx.setState(FOLLOWER);
        }

      }

      leader = Optional.of(Replica.fromString(request.getLeaderId()));
      resetTimer();
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
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {

    boolean voteGranted = false;

    if (request.getTerm() >= log.currentTerm()) {

      if (request.getTerm() > log.currentTerm()) {

        log.currentTerm(request.getTerm());

        if (ctx.getState().equals(LEADER) || ctx.getState().equals(CANDIDATE)) {
          ctx.setState(FOLLOWER);
        }

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
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    if (ctx.getState().equals(FOLLOWER)) {
      throw new NotLeaderException(leader.get());
    } else if (ctx.getState().equals(CANDIDATE)) {
      throw new NoLeaderException();
    }
    return Futures.immediateCancelledFuture();
  }
}
