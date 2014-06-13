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

import static org.robotninjas.barge.state.RaftStateContext.StateType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * An interface for being notified of state transitions within a given Raft instance.
 */
public interface StateTransitionListener {

  /**
   * Called when an instance has just transitioned between states.
   *
   * @param context the context originating the state transition. Useful when several replica instances might live within
   *                the same JVM space and notifies a single listener.
   * @param from    previous state of replica.
   * @param to      current state of replica.
   */
  void changeState(@Nonnull Raft context, @Nullable StateType from, @Nonnull StateType to);

  /**
   * Called when a transition is requested from an invalid state.
   *
   * @param context  the context originating the state transition. Useful when several replica instances might live within
   *                 the same JVM space and notifies a single listener.
   * @param actual   the actual state registered in context.
   * @param expected the expected state as requested by transition.
   */
  void invalidTransition(@Nonnull Raft context, @Nonnull StateType actual, @Nullable StateType expected);

  /**
   * Called when raft instance is stopped.
   *
   * @param raft the raft instance which is stopping.
   */
  void stop(@Nonnull Raft raft);
}
