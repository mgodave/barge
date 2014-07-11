package org.robotninjas.barge.state;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;


@NotThreadSafe
class RaftStateContext implements Raft {

  private final StateFactory stateFactory;
  private final Executor executor;
  private final String name;

  private final Set<StateTransitionListener> listeners = Sets.newConcurrentHashSet();
  private final Set<RaftProtocolListener> protocolListeners = Sets.newConcurrentHashSet();

  private volatile StateType state;
  private volatile State delegate;

  private boolean stop;

  @Inject
  RaftStateContext(String name, StateFactory stateFactory, @RaftExecutor Fiber executor, Set<StateTransitionListener> listeners, Set<RaftProtocolListener> protocolListeners) {
    MDC.put("self", name);

    this.stateFactory = stateFactory;
    this.executor = executor;
    this.name = name;

    this.listeners.add(new LogListener());
    this.listeners.addAll(listeners);
    this.protocolListeners.addAll(protocolListeners);
  }

  RaftStateContext(RaftLog log, StateFactory stateFactory, Fiber executor, Set<StateTransitionListener> listeners) {
    this(log.self().toString(), stateFactory, executor, listeners);
  }

  RaftStateContext(String name, StateFactory stateFactory, Fiber executor, Set<StateTransitionListener> listeners) {
    this(name, stateFactory, executor, listeners, Collections.<RaftProtocolListener>emptySet());
  }

  @Override
  public ListenableFuture<StateType> init() {
    ListenableFutureTask<StateType> init = ListenableFutureTask.create(new Callable<StateType>() {
      @Override
      public StateType call() {
        setState(null, StateType.START);

        return StateType.START;
      }
    });

    executor.execute(init);

    notifiesInit();

    return init;
  }

  @Override
  @Nonnull
  public RequestVoteResponse requestVote(@Nonnull final RequestVote request) {

    checkNotNull(request);

    ListenableFutureTask<RequestVoteResponse> response = ListenableFutureTask.create(new Callable<RequestVoteResponse>() {
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
    } finally {
      notifyRequestVote(request);
    }

  }

  @Override
  @Nonnull
  public AppendEntriesResponse appendEntries(@Nonnull final AppendEntries request) {

    checkNotNull(request);

    ListenableFutureTask<AppendEntriesResponse> response = ListenableFutureTask.create(new Callable<AppendEntriesResponse>() {
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
    } finally {
      notifyAppendEntries(request);
    }

  }


  @Override
  @Nonnull
  public ListenableFuture<Object> commitOperation(@Nonnull final byte[] op) throws RaftException {

    checkNotNull(op);

    ListenableFutureTask<Object> response = ListenableFutureTask.create(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return delegate.commitOperation(RaftStateContext.this, op);
      }
    });

    executor.execute(response);

    notifyCommit(op);

    return response;
  }

  public synchronized void setState(State oldState, @Nonnull StateType state) {

    if (this.delegate != oldState) {
      notifiesInvalidTransition(oldState);
      throw new IllegalStateException();
    }

    if (stop) {
      state = StateType.STOPPED;
      notifiesStop();
    }

    if (this.delegate != null) {
      this.delegate.destroy(this);
    }

    this.state = checkNotNull(state);

    delegate = stateFactory.makeState(state);

    MDC.put("state", this.state.toString());

    notifiesChangeState(oldState);

    delegate.init(this);
  }

  @Override
  public void addTransitionListener(@Nonnull StateTransitionListener transitionListener) {
    listeners.add(transitionListener);
  }

  @Override public void addRaftProtocolListener(@Nonnull RaftProtocolListener protocolListener) {
    protocolListeners.add(protocolListener);
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

  @Override
  public String toString() {
    return name;
  }

  private void notifiesStop() {

    for (StateTransitionListener listener : listeners) {
      listener.stop(this);
    }

  }

  private void notifiesInvalidTransition(State oldState) {

    for (StateTransitionListener listener : listeners) {
      listener.invalidTransition(this, state, (oldState == null) ? null : oldState.type());
    }
  }

  private void notifiesChangeState(State oldState) {

    for (StateTransitionListener listener : listeners) {
      listener.changeState(this, (oldState == null) ? null : oldState.type(), state);
    }
  }

  private void notifiesInit() {
    for (RaftProtocolListener protocolListener : protocolListeners) {
      protocolListener.init(this);
    }
  }

  private void notifyAppendEntries(AppendEntries request) {
    for (RaftProtocolListener protocolListener : protocolListeners) {
      protocolListener.appendEntries(this, request);
    }
  }

  private void notifyRequestVote(RequestVote vote) {
    for (RaftProtocolListener protocolListener : protocolListeners) {
      protocolListener.requestVote(this, vote);
    }
  }

  private void notifyCommit(byte[] bytes) {
    for (RaftProtocolListener protocolListener : protocolListeners) {
      protocolListener.commit(this, bytes);
    }
  }

}
