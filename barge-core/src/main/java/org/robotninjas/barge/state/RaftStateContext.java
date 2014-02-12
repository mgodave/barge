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

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.robotninjas.barge.proto.RaftProto.*;

@NotThreadSafe
class RaftStateContext implements Raft {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);

  private final StateFactory stateFactory;
  private final Executor executor;
  private final Set<StateTransitionListener> listeners = Sets.newConcurrentHashSet();

  private volatile StateType state;
  private volatile State delegate;

  private boolean stop;

  @Inject
  RaftStateContext(StateFactory stateFactory, @RaftExecutor Fiber executor) {
    this.stateFactory = stateFactory;
    this.executor = executor;
    this.listeners.add(new LogListener());
  }

  @Override
  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull final RequestVote request) {

    checkNotNull(request);

    ListenableFutureTask<RequestVoteResponse> response =
        ListenableFutureTask.create(new Callable<RequestVoteResponse>() {
          @Override
          public RequestVoteResponse call() throws Exception {
            return delegate.requestVote(RaftStateContext.this, request);
          }
        });

    executor.execute(response);

    try {
      return response.get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  @Override
  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull final AppendEntries request) {

    checkNotNull(request);

    ListenableFutureTask<AppendEntriesResponse> response =
        ListenableFutureTask.create(new Callable<AppendEntriesResponse>() {
          @Override
          public AppendEntriesResponse call() throws Exception {
            return delegate.appendEntries(RaftStateContext.this, request);
          }
        });

    executor.execute(response);

    try {
      return response.get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }

  @Override
  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull final byte[] op) throws RaftException {

    checkNotNull(op);

    ListenableFutureTask<Object> response =
        ListenableFutureTask.create(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            return delegate.commitOperation(RaftStateContext.this, op);
          }
        });

    executor.execute(response);

    return response;

  }

  @Override
  public synchronized void setState(State oldState, @Nonnull StateType state) {

    if (this.delegate != oldState) {
      notifiesInvalidTransition(oldState);
      throw new IllegalStateException();
    }

    if (stop) {
      state = StateType.STOPPED;
      LOGGER.info("Service stopping); replaced state with {}", state);
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

  public synchronized void stop() {
    stop = true;
    if (this.delegate != null) {
      this.delegate.doStop(this);
    }
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
