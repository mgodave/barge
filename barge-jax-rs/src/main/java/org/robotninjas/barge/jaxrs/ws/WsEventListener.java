package org.robotninjas.barge.jaxrs.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import org.jetlang.core.RunnableExecutorImpl;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.RaftProtocolListener;
import org.robotninjas.barge.state.StateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;


/**
 */
public class WsEventListener implements StateTransitionListener, RaftProtocolListener {

  private final Logger logger = LoggerFactory.getLogger(WsEventListener.class);

  private final Fiber executor;
  private final ObjectMapper json = new ObjectMapper();

  private Set<Listener> remotes = Sets.newConcurrentHashSet();

  public WsEventListener() {
    this(new ThreadFiber(new RunnableExecutorImpl(), "ws-events", true));
  }

  public WsEventListener(Fiber fiber) {
    this.executor = fiber;
  }

  public void start() {
    executor.start();
  }

  public void stop() {
    executor.dispose();
  }

  @Override
  public void changeState(@Nonnull final Raft context, @Nullable final Raft.StateType from, @Nonnull final Raft.StateType to) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        WsMessage message = WsMessages.stateChange(context, from, to);

        for (Listener remote : remotes) {
          send(message, remote);
        }
      }
    });
  }

  @Override
  public void invalidTransition(@Nonnull final Raft context, @Nonnull final Raft.StateType actual,
                                @Nullable final Raft.StateType expected) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        WsMessage message = WsMessages.invalidTransition(context, expected, actual);

        for (Listener remote : remotes) {
          send(message, remote);
        }
      }
    });
  }

  @Override
  public void stop(@Nonnull final Raft raft) {
    executor.execute(new Runnable() {
      @Override
      public void run() {
        WsMessage message = WsMessages.stopping(raft);

        for (Listener remote : remotes) {
          send(message, remote);
        }
      }
    });
  }

  @Override public void init(Raft raft) {
    
  }

  @Override public void appendEntries(@Nonnull Raft raft, @Nonnull AppendEntries entries) {

  }

  @Override public void requestVote(@Nonnull Raft raft, @Nonnull RequestVote vote) {

  }

  @Override public void commit(@Nonnull Raft raft, @Nonnull byte[] operation) {

  }


  public void addClient(Listener listener) {
    logger.info("adding listener: {}", listener);

    remotes.add(listener);
  }


  public void removeClient(Listener listener) {
    logger.info("removing listener: {}", listener);

    remotes.remove(listener);
  }

  public void error(EventSocket socket, Throwable error) {
    logger.warn("caught error on socket {}", socket, error);
  }

  private void send(WsMessage message, Listener remote) {
    try {
      remote.send(json.writeValueAsString(message));
    } catch (JsonProcessingException e) {
      logger.warn("fail to convert {} to json", message, e);
    }
  }

}
