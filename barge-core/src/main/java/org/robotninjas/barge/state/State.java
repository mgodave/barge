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

import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

import javax.annotation.Nonnull;

import static org.robotninjas.barge.state.RaftStateContext.StateType;

interface State {

  void init(@Nonnull RaftStateContext ctx);

  void destroy(@Nonnull RaftStateContext ctx);

  @Nonnull
  RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request);

  @Nonnull
  AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request);

  @Nonnull
  ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException;

  void doStop(RaftStateContext ctx);

  StateType type();
}
