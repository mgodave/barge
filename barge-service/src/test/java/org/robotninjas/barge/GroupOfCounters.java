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

import com.google.common.collect.Lists;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.util.List;

/**
 * User: arnaud
 * Date: 03/02/14
 * Time: 08:58
 */
public class GroupOfCounters extends ExternalResource {

  private final List<Replica> replicas;
  private final List<SimpleCounterMachine> counters;
  private final File target;

  public GroupOfCounters(int numberOfReplicas, File target) {
    this.target = target;

    replicas = Lists.newArrayList();
    counters = Lists.newArrayList();

    for (int i = 10001; i <= (10000 + numberOfReplicas); i++) {
      replicas.add(Replica.fromString("localhost:" + i));
      counters.add(new SimpleCounterMachine(i - 10001, replicas));
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
    counters.get(0).commit(bytes);
  }

  /**
   * Wait for all {@link SimpleCounterMachine} in the cluster to reach a consensus value.
   *
   * @param target expected value for each machine' counter.
   * @param timeout timeout in ms. Timeout is evaluated per instance of counter within the cluster.
   */
  public void waitAllToReachValue(int target, long timeout) {
    for (SimpleCounterMachine counter : counters) {
       counter.waitForValue(target,timeout);
    }
  }

  void waitForLeaderElection() throws InterruptedException {
    // TODO replace sleep with observation of leader election transition
    Thread.sleep(60000);
  }
}
