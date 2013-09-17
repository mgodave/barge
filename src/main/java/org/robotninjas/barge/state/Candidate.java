package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.barge.context.RaftContext;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RpcClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.google.common.base.Predicates.notNull;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.rpc.RaftProto.*;
import static org.robotninjas.barge.state.Candidate.CountVotesFunction.countVotes;
import static org.robotninjas.barge.state.Candidate.IsElectedFunction.isElected;
import static org.robotninjas.barge.state.Candidate.VoteGrantedPredicate.voteGranted;
import static org.robotninjas.barge.state.Context.StateType.*;

public class Candidate implements State {

  private static final Logger LOGGER = LoggerFactory.getLogger(Candidate.class);
  private static final Random RAND = new Random(System.nanoTime());

  private final RaftContext rctx;
  private final ScheduledExecutorService scheduler;
  private final long electionTimeout;
  private final RpcClientProvider clientProvider;
  private ScheduledFuture<?> electionTimer;
  ListenableFuture<Boolean> electionResult;

  @Inject
  Candidate(RaftContext rctx, @RaftScheduler ScheduledExecutorService scheduler,
            @ElectionTimeout long electionTimeout, RpcClientProvider clientProvider) {
    this.rctx = rctx;
    this.scheduler = scheduler;
    this.electionTimeout = electionTimeout;
    this.clientProvider = clientProvider;
  }

  @Override
  public void init(final Context ctx) {

    rctx.term(rctx.term() + 1);
    rctx.votedFor(Optional.of(rctx.self()));

    LOGGER.debug("Election starting for term {}", rctx.term());

    List<ListenableFuture<RequestVoteResponse>> responses = sendRequests();
    ListenableFuture<List<RequestVoteResponse>> successful = successfulAsList(responses);
    ListenableFuture<Integer> votes = transform(successful, countVotes());
    electionResult = transform(votes, isElected(rctx.log().members().size() + 1));

    electionResult.addListener(new Runnable() {
      @Override
      public void run() {
        if (getUnchecked(electionResult)) {
          transition(ctx, LEADER);
        }
      }
    }, sameThreadExecutor());

    long timeout = electionTimeout + (RAND.nextLong() % electionTimeout);
    electionTimer = scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Election timeout");
        transition(ctx, CANDIDATE);
      }
    }, timeout, MILLISECONDS);
  }

  private void transition(Context ctx, Context.StateType state) {
    ctx.setState(state);
    electionResult.cancel(false);
    electionTimer.cancel(false);
  }

  private void stepDown(Context ctx) {
    transition(ctx, FOLLOWER);
  }

  @Override
  public RequestVoteResponse requestVote(Context ctx, RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    Replica candidate = Replica.fromString(request.getCandidateId());

    boolean voteGranted = false;

    if (request.getTerm() > rctx.term()) {
      rctx.term(request.getTerm());
      stepDown(ctx);
      voteGranted = Voting.shouldVoteFor(rctx, request);
      if (voteGranted) {
        rctx.votedFor(Optional.of(candidate));
      }
    }

    return RequestVoteResponse.newBuilder()
      .setTerm(rctx.term())
      .setVoteGranted(voteGranted)
      .build();

  }

  @Override
  public AppendEntriesResponse appendEntries(Context ctx, AppendEntries request) {

    LOGGER.debug("AppendEntries received for term {}", request.getTerm());

    boolean success = false;

    if (request.getTerm() >= rctx.term()) {

      if (request.getTerm() > rctx.term()) {
        rctx.term(request.getTerm());
      }

      stepDown(ctx);

      success = rctx.log().append(request);

    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(rctx.term())
      .setSuccess(success)
      .build();

  }

  @VisibleForTesting
  List<ListenableFuture<RequestVoteResponse>> sendRequests() {
    List<ListenableFuture<RequestVoteResponse>> responses = Lists.newArrayList();
    for (Replica replica : rctx.log().members()) {
      RaftClient client = clientProvider.get(replica);
      RequestVote request =
        RequestVote.newBuilder()
          .setTerm(rctx.term())
          .setCandidateId(rctx.self().toString())
          .setLastLogIndex(rctx.log().lastLogIndex())
          .setLastLogTerm(rctx.log().lastLogTerm())
          .build();
      responses.add(client.requestVote(request));
    }
    return responses;
  }

  @Immutable
  static class IsElectedFunction implements Function<Integer, Boolean> {

    private final int membershipCount;

    private IsElectedFunction(int membershipCount) {
      this.membershipCount = membershipCount;
    }

    @Nullable
    @Override
    public Boolean apply(@Nullable Integer input) {
      return input > (membershipCount / 2.0);
    }

    static Function<Integer, Boolean> isElected(int membershipCount) {
      return new IsElectedFunction(membershipCount);
    }

  }

  @Immutable
  static enum CountVotesFunction implements Function<List<RequestVoteResponse>, Integer> {

    CountVotes;

    @Override
    public Integer apply(List<RequestVoteResponse> responses) {
      return FluentIterable
        .from(responses)
        .filter(notNull())
        .filter(voteGranted())
        .size() + 1;
    }

    static Function<List<RequestVoteResponse>, Integer> countVotes() {
      return CountVotes;
    }

  }

  @Immutable
  static enum VoteGrantedPredicate implements Predicate<RequestVoteResponse> {

    Granted(true),
    NotGranted(false);

    boolean offset;

    VoteGrantedPredicate(boolean offset) {
      this.offset = offset;
    }

    @Override
    public boolean apply(RequestVoteResponse input) {
      return input.getVoteGranted() && offset;
    }

    static Predicate<RequestVoteResponse> voteGranted() {
      return Granted;
    }

    static Predicate<RequestVoteResponse> voteNotGranted() {
      return NotGranted;
    }

  }

}
