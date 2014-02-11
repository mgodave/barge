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

import com.google.inject.Inject;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.*;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;

@NotThreadSafe
class Follower extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Follower.class);

  private final Fiber scheduler;
  private final long timeout;
  private DeadlineTimer timeoutTask;

  @Inject
  Follower(RaftLog log, @RaftExecutor Fiber scheduler, @ElectionTimeout @Nonnegative long timeout) {

    super(FOLLOWER, log);

    this.scheduler = checkNotNull(scheduler);
    checkArgument(timeout >= 0);
    this.timeout = timeout;

  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {
    timeoutTask = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("DeadlineTimer expired, starting election");
        ctx.setState(Follower.this, CANDIDATE);
      }
    }, timeout * 2);
  }

  @Override
  public void destroy(RaftStateContext ctx) {
    timeoutTask.cancel();
  }


  protected void resetTimer() {
    timeoutTask.reset();
  }

  @Override
  public void doStop(RaftStateContext ctx) {
    timeoutTask.cancel();
    super.doStop(ctx);
  }

}
