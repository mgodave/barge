package org.robotninjas.barge.state;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Runnables.doNothing;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.MDC;


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
    this(name, stateFactory, executor, listeners, Collections.emptySet());
  }

  @Override
  public CompletableFuture<StateType> init() {
    return CompletableFuture.supplyAsync(() -> {
      setState(null, StateType.START);
      notifiesInit();
      return StateType.START;
    }, executor);
  }

  @Override
  @Nonnull
  public CompletableFuture<RequestVoteResponse> requestVote(@Nonnull final RequestVote request) {

    checkNotNull(request);

    CompletableFuture<RequestVoteResponse> response = CompletableFuture
        .runAsync(doNothing(), executor)
        .thenApply(ignored ->
            delegate.requestVote(RaftStateContext.this, request)
        );

    response.handle((i, e) -> {
      notifyRequestVote(request);
      return null;
    });

    return response;
  }

  @Override
  @Nonnull
  public CompletableFuture<AppendEntriesResponse> appendEntries(@Nonnull final AppendEntries request) {
    checkNotNull(request);

    CompletableFuture<AppendEntriesResponse> response = CompletableFuture
        .runAsync(doNothing(), executor)
        .thenApply(ignored ->
            delegate.appendEntries(RaftStateContext.this, request)
        );

    response.handle((i, e) -> {
        notifyAppendEntries(request);
        return null;
    });

    return response;

  }


  @Override
  @Nonnull
  public CompletableFuture<Object> commitOperation(@Nonnull final byte[] op) {
    checkNotNull(op);

    CompletableFuture<Object> response = CompletableFuture
        .runAsync(doNothing(), executor)
        .thenComposeAsync(ignored ->
            delegate.commitOperation(RaftStateContext.this, op)
        );

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

    try {
      stateFactory.close();
    } catch (IOException ignored) {
    }
  }

  @Override
  public String toString() {
    return name;
  }

  private void notifiesStop() {
    executor.execute(() -> {
      for (StateTransitionListener listener : listeners) {
        listener.stop(this);
      }
    });
  }

  private void notifiesInvalidTransition(State oldState) {
    executor.execute(() -> {
      for (StateTransitionListener listener : listeners) {
        listener.invalidTransition(this, state, (oldState == null) ? null : oldState.type());
      }
    });
  }

  private void notifiesChangeState(State oldState) {
    executor.execute(() -> {
      for (StateTransitionListener listener : listeners) {
        listener.changeState(this, (oldState == null) ? null : oldState.type(), state);
      }
    });
  }

  private void notifiesInit() {
    executor.execute(() -> {
      for (RaftProtocolListener protocolListener : protocolListeners) {
        protocolListener.init(this);
      }
    });
  }

  private void notifyAppendEntries(AppendEntries request) {
    executor.execute(() -> {
      for (RaftProtocolListener protocolListener : protocolListeners) {
        protocolListener.appendEntries(this, request);
      }
    });
  }

  private void notifyRequestVote(RequestVote vote) {
    executor.execute(() -> {
      for (RaftProtocolListener protocolListener : protocolListeners) {
        protocolListener.requestVote(this, vote);
      }
    });
  }

  private void notifyCommit(byte[] bytes) {
    executor.execute(() -> {
      for (RaftProtocolListener protocolListener : protocolListeners) {
        protocolListener.commit(this, bytes);
      }
    });
  }

}
