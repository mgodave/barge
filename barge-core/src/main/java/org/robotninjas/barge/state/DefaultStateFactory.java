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

import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;

import javax.annotation.Nonnegative;
import javax.inject.Inject;

class DefaultStateFactory implements StateFactory {


  private final RaftLog log;
  private final Fiber scheduler;
  private final long timeout;
  private final ReplicaManagerFactory replicaManagerFactory;
  private final Client client;

  @Inject
  public DefaultStateFactory(RaftLog log, @RaftExecutor Fiber scheduler,
                             @ElectionTimeout @Nonnegative long timeout, ReplicaManagerFactory replicaManagerFactory,
                             Client client) {
    this.log = log;
    this.scheduler = scheduler;
    this.timeout = timeout;
    this.replicaManagerFactory = replicaManagerFactory;
    this.client = client;
  }

  @Override
  public State makeState(RaftStateContext.StateType state) {
    switch (state) {
      case START:
        return new Start(log);
      case FOLLOWER:
        return new Follower(log, scheduler, timeout);
      case LEADER:
        return new Leader(log, scheduler, timeout, replicaManagerFactory);
      case CANDIDATE:
        return new Candidate(log, scheduler, timeout, client);
      case STOPPED:
        return new Stopped(log);
      default:
        throw new IllegalStateException("the impossible happpened, unknown state type " + state);
    }
  }
}
