package org.robotninjas.barge.state;

import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.ThreadFiber;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.RequestVote;

import java.util.Collections;

import static org.mockito.Mockito.*;
import static org.robotninjas.barge.state.Raft.StateType.*;


public class RaftStateContextTest {

  private final StateFactory factory = mock(StateFactory.class);

  private final State start = mock(State.class);
  private final State follower = mock(State.class);
  private final State candidate = mock(State.class);
  private final Fiber executor = new ThreadFiber();

  private final StateTransitionListener transitionListener = mock(StateTransitionListener.class);
  private final RaftProtocolListener protocolListener = mock(RaftProtocolListener.class);

  private final RaftStateContext context = new RaftStateContext("mockstatecontext", factory, executor, Collections.<StateTransitionListener>emptySet(), Collections.<RaftProtocolListener>emptySet());

  @Before
  public void setup() {
    when(factory.makeState(START)).thenReturn(start);
    when(factory.makeState(FOLLOWER)).thenReturn(follower);
    executor.start();
  }

  @Test
  public void notifiesSuccessfulStateTransitionFromNullToStartToRegisteredListener() throws Exception {
    context.addTransitionListener(transitionListener);

    context.setState(null, START);

    verify(transitionListener).changeState(context, null, START);
  }

  @Test
  public void notifiesInitToRegisteredProtocolListenerWhenInitialized() throws Exception {
    context.addRaftProtocolListener(protocolListener);

    context.init();

    verify(protocolListener).init(context);
  }

  @Test
  public void notifiesAppendEntriesToRegisteredProtocolListenerWhenReceivingAppendEntries() throws Exception {
    AppendEntries entries = AppendEntries.getDefaultInstance();

    context.addRaftProtocolListener(protocolListener);
    context.setState(null, FOLLOWER);
    context.appendEntries(entries);

    verify(protocolListener).appendEntries(context, entries);
  }

  @Test
  public void notifiesRequestVoteToRegisteredProtocolListenersWhenReceivingRequestVote() throws Exception {
    RequestVote vote = RequestVote.getDefaultInstance();
    
    context.addRaftProtocolListener(protocolListener);
    context.setState(null, FOLLOWER);
    context.requestVote(vote);
    
    verify(protocolListener).requestVote(context, vote);
  }

  @Test
  public void notifiesCommitToRegisteredProtocolListenerWhenReceivingCommit() throws Exception {
    byte[] bytes = new byte[]{(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe};

    context.addRaftProtocolListener(protocolListener);
    context.setState(null, FOLLOWER);
    context.commitOperation(bytes);

    verify(protocolListener).commit(context, bytes);   
  }

  @Test
  public void throwsExceptionAndNotifiesInvalidTransitionToRegisteredListenerGivenUnexpectedPreviousState() throws Exception {
    when(candidate.type()).thenReturn(CANDIDATE);
    context.addTransitionListener(transitionListener);

    context.setState(null, START);

    try {
      context.setState(candidate, LEADER);
    } catch (IllegalStateException e) {
      verify(transitionListener).invalidTransition(context, START, CANDIDATE);
    }
  }

  @Test
  public void triggersInitOnNewStateWhenTransitioning() throws Exception {
    context.setState(null, START);

    verify(start).init(context);
  }

  @Test
  public void delegatesAppendEntriesRequestToCurrentState() throws Exception {
    AppendEntries appendEntries = AppendEntries.getDefaultInstance();
    context.setState(null, FOLLOWER);

    context.appendEntries(appendEntries);

    verify(follower).appendEntries(context, appendEntries);
  }

  @Test
  public void delegateRequestVoteToCurrentState() throws Exception {
    RequestVote requestVote = RequestVote.getDefaultInstance();
    context.setState(null, FOLLOWER);

    context.requestVote(requestVote);

    verify(follower).requestVote(context, requestVote);
  }

  @Test
  @Ignore
  public void delegateCommitOperationToCurrentState() throws Exception {
    byte[] bytes = new byte[]{1};
    context.setState(null, FOLLOWER);

    context.commitOperation(bytes);

    verify(follower).commitOperation(context, bytes);
  }
}
