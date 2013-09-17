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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;

import static org.robotninjas.barge.rpc.RaftProto.*;

public class Context {

  static enum StateType {FOLLOWER, CANDIDATE, LEADER};

  private static final Logger LOGGER = LoggerFactory.getLogger(Context.class);

  private final StateFactory stateFactory;
  private volatile StateType state;
  private volatile State delegate;

  @Inject
  Context(StateFactory stateFactory) {
    this.stateFactory = stateFactory;
    init();
  }

  public void init() {
    delegate = stateFactory.follower();
    state = StateType.FOLLOWER;
    delegate.init(this);
    MDC.put("state", state.toString());
  }

  public RequestVoteResponse requestVote(RequestVote request) {
    return delegate.requestVote(this, request);
  }

  public AppendEntriesResponse appendEntries(AppendEntries request) {
    return delegate.appendEntries(this, request);
  }

  void setState(StateType state) {
    MDC.put("state", state.toString());
    LOGGER.debug("old state: {}, new state: {}", this.state, state);
    this.state = state;
    switch (state) {
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
    delegate.init(this);
  }

  @VisibleForTesting
  StateType getState() {
    return state;
  }

}
