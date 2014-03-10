package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.state.Raft.StateType.*;
import static org.robotninjas.barge.state.RaftStateContext.StateType;

public abstract class BaseState implements State {

  private final StateType type;
  private final RaftLog log;
  private Optional<Replica> leader;

  protected BaseState(@Nullable StateType type, @Nonnull RaftLog log) {
    this.log = checkNotNull(log);
    this.type = type;
  }

  @Nullable
  @Override
  public StateType type() {
    return type;
  }

  protected RaftLog getLog() {
    return log;
  }

  @Override
  public void destroy(RaftStateContext ctx) {
  }

  @VisibleForTesting
  boolean shouldVoteFor(@Nonnull RaftLog log, @Nonnull RequestVote request) {

    //  If votedFor is null or candidateId, and candidate's log is at 
    //  least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
      
    Optional<Replica> votedFor = log.votedFor();
    Replica candidate = log.getReplica(request.getCandidateId());

    if (votedFor.isPresent()) {
      if (!votedFor.get().equals(candidate)) {
        return false;
      }
    }
    
    assert !votedFor.isPresent() || votedFor.get().equals(candidate);
    
    boolean logIsComplete;
    if (request.getLastLogTerm() > log.lastLogTerm()) {
      logIsComplete = true;
    } else if (request.getLastLogTerm() == log.lastLogTerm()) {
      if (request.getLastLogIndex() >= log.lastLogIndex()) {
        logIsComplete = true;
      } else {
        logIsComplete = false;
      }
    } else {
      logIsComplete = false;
    }
    
    if (logIsComplete) {
      // Requestor has an up-to-date log, we haven't voted for anyone else => OK
      return true;
    }
   
    return false;
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

        if (ctx.type().equals(LEADER) || ctx.type().equals(CANDIDATE)) {
          ctx.setState(this, FOLLOWER);
        }

      }

      leader = Optional.of(log.getReplica(request.getLeaderId()));
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

    boolean voteGranted;

    long term = request.getTerm();
    long currentTerm = log.currentTerm();
    
    if (term < currentTerm) {
      // Reply false if term < currentTerm (§5.1)
      voteGranted = false;
    } else {

      // If RPC request or response contains term T > currentTerm:
      // set currentTerm = T, convert to follower (§5.1)
      if (term > currentTerm) {

        log.currentTerm(term);

        if (ctx.type().equals(LEADER) || ctx.type().equals(CANDIDATE)) {
          ctx.setState(this, FOLLOWER);
        }

      }

      Replica candidate = log.getReplica(request.getCandidateId());
      voteGranted = shouldVoteFor(log, request);

      if (voteGranted) {
        log.votedFor(Optional.of(candidate));
      }

    }

    return RequestVoteResponse.newBuilder()
        .setTerm(currentTerm)
        .setVoteGranted(voteGranted)
        .build();

  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    StateType stateType = ctx.type();
    Preconditions.checkNotNull(stateType);
    if (stateType.equals(FOLLOWER)) {
      throw new NotLeaderException(leader.get());
    } else if (stateType.equals(CANDIDATE)) {
      throw new NoLeaderException();
    }
    return Futures.immediateCancelledFuture();
  }
  
  @Override
  public void doStop(RaftStateContext ctx) {
    ctx.setState(this, STOPPED);
  }

}
