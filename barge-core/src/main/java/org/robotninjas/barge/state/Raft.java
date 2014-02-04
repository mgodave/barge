/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
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

import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftException;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.proto.RaftProto.*;

/**
 * Main interface to a Raft protocol instance.
 */
public interface Raft {

  @Nonnull
  RequestVoteResponse requestVote(@Nonnull RequestVote request);

  @Nonnull
  AppendEntriesResponse appendEntries(@Nonnull AppendEntries request);

  @Nonnull
  ListenableFuture<Object> commitOperation(@Nonnull byte[] op) throws RaftException;

  void setState(State oldState, @Nonnull StateType state);

  void addTransitionListener(@Nonnull StateTransitionListener transitionListener);

  @Nonnull
  StateType type();

  public static enum StateType {START, FOLLOWER, CANDIDATE, LEADER}
}
