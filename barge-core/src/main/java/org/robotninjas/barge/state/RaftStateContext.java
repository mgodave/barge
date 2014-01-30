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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
class RaftStateContext implements Raft {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  private final StateFactory stateFactory;
  private final Set<StateTransitionListener> listeners = Sets.newConcurrentHashSet();

  private volatile StateType state;
  private volatile State delegate;

  @Inject
  RaftStateContext(StateFactory stateFactory) {
    this.stateFactory = stateFactory;
    this.listeners.add(new LogListener());
  }

  @Override
  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull RequestVote request) {
    checkNotNull(request);
    return delegate.requestVote(this, request);
  }

  @Override
  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull AppendEntries request) {
    checkNotNull(request);
    return delegate.appendEntries(this, request);
  }

  @Override
  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull byte[] op) throws RaftException {
    checkNotNull(op);
    return delegate.commitOperation(this, op);
  }

  @Override
  public synchronized void setState(State oldState, @Nonnull StateType state) {

    if (this.delegate != oldState) {
      notifiesInvalidTransition(oldState);
      throw new IllegalStateException();
    }

    LOGGER.info("old state: {}, new state: {}", this.state, state);
    if (this.delegate != null) {
      this.delegate.destroy(this);
    }

    this.state = checkNotNull(state);

    delegate = stateFactory.makeState(state);

    MDC.put("state", this.state.toString());

    notifiesChangeState(oldState);

    delegate.init(this);
  }

  private void notifiesInvalidTransition(State oldState) {
    for (StateTransitionListener listener : listeners) {
      listener.invalidTransition(this, state, oldState == null ? null : oldState.type());
    }
  }

  private void notifiesChangeState(State oldState) {
    for (StateTransitionListener listener : listeners) {
      listener.changeState(this, oldState == null ? null : oldState.type(), state);
    }
  }

  @Override
  public void addTransitionListener(@Nonnull StateTransitionListener transitionListener) {
    listeners.add(transitionListener);
  }

  @Override
  @Nonnull
  public StateType type() {
    return state;
  }

  private class LogListener implements StateTransitionListener {
    @Override
    public void changeState(@Nonnull Raft context, @Nullable StateType from, @Nonnull StateType to) {
      LOGGER.info("old state: {}, new state: {}", from, to);
    }

    @Override
    public void invalidTransition(@Nonnull Raft context, @Nonnull StateType actual, @Nullable StateType expected) {
      LOGGER.warn("State transition from incorrect previous state.  Expected {}, was {}", actual, expected);
    }
  }
}
