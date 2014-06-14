package org.robotninjas.barge.jaxrs.ws;

import org.jetlang.fibers.FiberStub;
import org.junit.Test;
import org.robotninjas.barge.state.Raft;

import static org.mockito.Mockito.*;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.LEADER;
import static org.robotninjas.barge.state.Raft.StateType.START;

public class WsEventListenerTest {

  private final FiberStub fiber = spy(new FiberStub());
  private final WsEventListener wsEventListener = new WsEventListener(fiber);
  private final Listener listener = mock(Listener.class);
  private final Raft raft = mock(Raft.class);

  @Test
  public void starts_executor_when_starting() throws Exception {
    wsEventListener.start();

    verify(fiber).start();
  }

  @Test
  public void disposes_executor_when_stopping() throws Exception {
    wsEventListener.stop();

    verify(fiber).dispose();
  }

  @Test
  public void dispatch_runnable_notifying_state_change_to_registered_listener_when_state_changes() throws Exception {
    wsEventListener.addClient(listener);
    wsEventListener.changeState(raft, CANDIDATE, LEADER);

    fiber.executeAllPending();

    verify(listener).send(anyString());
  }

  @Test
  public void dispatch_runnable_notifying_invalid_transition_to_registered_listener_when_transition_is_invalid() throws Exception {
    wsEventListener.addClient(listener);
    wsEventListener.invalidTransition(raft, LEADER, START);

    fiber.executeAllPending();

    verify(listener).send(anyString());
  }

  @Test
  public void dispatch_runnable_notifying_stop_to_registered_listener_when_stopping() throws Exception {
    wsEventListener.addClient(listener);
    wsEventListener.stop(raft);

    fiber.executeAllPending();

    verify(listener).send(anyString());
  }

  @Test
  public void removed_listener_does_not_receive_notifications_after_its_removal() throws Exception {
    wsEventListener.addClient(listener);
    wsEventListener.changeState(raft, CANDIDATE, LEADER);
    fiber.executeAllPending();
    
    wsEventListener.removeClient(listener);

    wsEventListener.changeState(raft, CANDIDATE, LEADER);
    fiber.executeAllPending();

    verify(listener, times(1)).send(anyString());
  }

}