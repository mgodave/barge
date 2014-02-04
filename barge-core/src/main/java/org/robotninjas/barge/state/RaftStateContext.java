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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
public class RaftStateContext {

  public enum StateType {START, FOLLOWER, CANDIDATE, LEADER}

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  private final StateFactory stateFactory;
  private volatile StateType state;
  private volatile State delegate;

  @Inject
  RaftStateContext(StateFactory stateFactory) {
    this.stateFactory = stateFactory;
  }

  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull RequestVote request) {
    checkNotNull(request);
    return delegate.requestVote(this, request);
  }

  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull AppendEntries request) {
    checkNotNull(request);
    return delegate.appendEntries(this, request);
  }

  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] op) throws RaftException {
    checkNotNull(op);
    return delegate.commitOperation(this, op);
  }

  public synchronized void setState(State oldState, @Nonnull StateType state) {
    if (this.delegate != oldState) {
      LOGGER.warn("State transition from incorrect previous state.  Expected {}, was {}",
              this.delegate,
              oldState);
      throw new IllegalStateException();
    }

    LOGGER.info("old state: {}, new state: {}", this.state, state);
    this.state = checkNotNull(state);
    switch (state) {
      case START:
        delegate = stateFactory.start();
        break;
      case FOLLOWER:
        delegate = stateFactory.follower();
        break;
      case LEADER:
        delegate = stateFactory.leader();
        break;
      case CANDIDATE:
        delegate = stateFactory.candidate();
        break;
    }
    MDC.put("state", this.state.toString());
    delegate.init(this);
  }

  @Nonnull
  public StateType getState() {
    return state;
  }
}
