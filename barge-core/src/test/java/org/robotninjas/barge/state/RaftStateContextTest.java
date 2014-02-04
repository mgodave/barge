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

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.robotninjas.barge.state.RaftStateContext.StateType.*;

public class RaftStateContextTest {

  private StateFactory factory = mock(StateFactory.class);
  private Start start = mock(Start.class);
  private Follower follower = mock(Follower.class);
  private Candidate candidate = mock(Candidate.class);

  private StateTransitionListener transitionListener = mock(StateTransitionListener.class);
  private RaftStateContext context = new RaftStateContext(factory);

  @Before
  public void setup() {
    when(factory.start()).thenReturn(start);
    when(start.type()).thenReturn(START);

    when(factory.follower()).thenReturn(follower);
    when(follower.type()).thenReturn(FOLLOWER);

    when(candidate.type()).thenReturn(CANDIDATE);
  }

  @Test
  public void notifiesSuccessfulStateTransitionFromNullToStartToRegisteredListener() throws Exception {
    context.addTransitionListener(transitionListener);

    context.setState(null, START);

    verify(transitionListener).changeState(context, null, START);
  }

  @Test
  public void throwsExceptionAndNotifiesInvalidTransitionToRegisteredListenerGivenUnexpectedPreviousState() throws Exception {
    context.addTransitionListener(transitionListener);

    context.setState(null, START);

    try  {
      context.setState(candidate, LEADER);
    } catch(IllegalStateException e) {
      verify(transitionListener).invalidTransition(context,START,CANDIDATE);
    }
  }

  @Test
  public void triggersInitOnNewStateWhenTransitioning() throws Exception {
    context.setState(null, START);

    verify(start).init(context);
  }


}
