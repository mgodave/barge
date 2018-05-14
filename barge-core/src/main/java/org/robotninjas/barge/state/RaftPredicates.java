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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVoteResponse;

@Immutable
class RaftPredicates {

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
  enum AppendSuccessPredicate implements Predicate<AppendEntriesResponse> {

    Success;

    @Override
    public boolean test(AppendEntriesResponse input) {
      checkNotNull(input);
      return input.getSuccess();    }
  }

  /**
   * Predicate returning true if the {@link RequestVoteResponse} returns voteGranted.
   */
  @Immutable
  @VisibleForTesting
  enum VoteGrantedPredicate implements Predicate<RequestVoteResponse> {

    VoteGranted;

    @Override
    public boolean test(@Nullable RequestVoteResponse input) {
      checkNotNull(input);
      return input.getVoteGranted();
    }

  }

}
