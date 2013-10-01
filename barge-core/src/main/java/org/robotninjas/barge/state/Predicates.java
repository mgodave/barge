package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.AppendEntriesResponse;
import static org.robotninjas.barge.proto.RaftProto.RequestVoteResponse;

@Immutable
class Predicates {

  @Nonnull
  static Predicate<AppendEntriesResponse> appendSuccessul() {
    return AppendSuccessPredicate.Success;
  }

  @Nonnull
  static Predicate<RequestVoteResponse> voteGranted() {
    return VoteGrantedPredicate.VoteGranted;
  }

  /**
   * Predicate returning true if the {@link AppendEntriesResponse} returns success.
   */
  @Immutable
  @VisibleForTesting
  static enum AppendSuccessPredicate implements Predicate<AppendEntriesResponse> {

    Success;

    @Override
    public boolean apply(@Nullable AppendEntriesResponse input) {
      checkNotNull(input);
      return input.getSuccess();
    }

  }

  /**
   * Predicate returning true if the {@link RequestVoteResponse} returns voteGranted.
   */
  @Immutable
  @VisibleForTesting
  static enum VoteGrantedPredicate implements Predicate<RequestVoteResponse> {

    VoteGranted;

    @Override
    public boolean apply(@Nullable RequestVoteResponse input) {
      checkNotNull(input);
      return input.getVoteGranted();
    }

  }

}
