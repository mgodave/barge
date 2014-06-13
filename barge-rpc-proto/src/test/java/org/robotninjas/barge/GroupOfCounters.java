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
package org.robotninjas.barge;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.rules.ExternalResource;

import org.robotninjas.barge.state.Raft;
import static org.robotninjas.barge.state.Raft.StateType;
import org.robotninjas.barge.state.StateTransitionListener;
import org.robotninjas.barge.utils.Prober;

import java.io.File;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class GroupOfCounters extends ExternalResource implements StateTransitionListener {

  private final List<NettyReplica> replicas;
  private final List<SimpleCounterMachine> counters;
  private final File target;
  private final Map<Raft, StateType> states = Maps.newConcurrentMap();

  public GroupOfCounters(int numberOfReplicas, File target) {
    this.target = target;

    replicas = Lists.newArrayList();
    counters = Lists.newArrayList();

    for (int i = 10001; i <= (10000 + numberOfReplicas); i++) {
      replicas.add(NettyReplica.fromString("localhost:" + i));
      counters.add(new SimpleCounterMachine(i - 10001, replicas, this));
    }
  }

  @Override
  protected void before() throws Throwable {

    for (SimpleCounterMachine counter : counters) {
      counter.makeLogDirectory(target);
      counter.startRaft();
    }
  }

  @Override
  protected void after() {

    for (SimpleCounterMachine counter : counters) {
      counter.stop();
      counter.deleteLogDirectory();
    }
  }

  public void commitToLeader(byte[] bytes) throws RaftException, InterruptedException {
    getLeader().get().commit(bytes);
  }

  private Optional<SimpleCounterMachine> getLeader() {

    for (SimpleCounterMachine counter : counters) {

      if (counter.isLeader()) {
        return Optional.of(counter);
      }
    }

    return Optional.absent();
  }

  /**
   * Wait for all {@link SimpleCounterMachine} in the cluster to reach a consensus value.
   *
   * @param target  expected value for each machine' counter.
   * @param timeout timeout in ms. Timeout is evaluated per instance of counter within the cluster.
   */
  public void waitAllToReachValue(int target, long timeout) {

    for (SimpleCounterMachine counter : counters) {
      counter.waitForValue(target, timeout);
    }
  }

  void waitForLeaderElection() throws InterruptedException {
    new Prober(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return thereIsOneLeader();
        }
      }).probe(10000);
  }

  private Boolean thereIsOneLeader() {
    int numberOfLeaders = 0;
    int numberOfFollowers = 0;

    for (StateType stateType : states.values()) {

      switch (stateType) {

        case LEADER:
          numberOfLeaders++;

          break;

        case FOLLOWER:
          numberOfFollowers++;

          break;
      }
    }

    return (numberOfLeaders == 1) && ((numberOfFollowers + numberOfLeaders) == replicas.size());
  }


  @Override
  public void changeState(@Nonnull Raft context, @Nullable StateType from, @Nonnull StateType to) {
    states.put(context, to);
  }

  @Override
  public void invalidTransition(@Nonnull Raft context, @Nonnull StateType actual, @Nullable StateType expected) {
    // IGNORED
  }

  @Override
  public void stop(@Nonnull Raft raft) {
    // IGNORED
  }
}
