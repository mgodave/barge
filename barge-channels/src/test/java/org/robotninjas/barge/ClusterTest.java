/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.robotninjas.barge;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_MINUTE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.robotninjas.barge.state.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnitQuickcheck.class)
public class ClusterTest {
    private static final ByteBuffer MESSAGE = ByteBuffer.wrap("Hi there".getBytes());
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTest.class);
    private static ScheduledExecutorService executor =
        newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("fiber-pool-%d")
                .setDaemon(true).build()
        );

    private static boolean everyoneElseIsAFollower(List<ChannelRaftService> cluster) {
        return cluster.stream()
            .map(ChannelRaftService::getState)
            .filter(s -> s == Raft.StateType.FOLLOWER)
            .count() == (cluster.size() - 1);
    }

    private static Boolean thereIsOneLeader(List<ChannelRaftService> cluster) {
        return cluster.stream()
            .map(ChannelRaftService::getState)
            .filter(s -> s == Raft.StateType.LEADER)
            .count() == 1;
    }

    private static Callable<Boolean> allReplicasHaveReceivedMessage(CountingStateMachine stateMachine) {
        return () -> stateMachine.getNumReplicas() <= 0;
    }

    @BeforeClass
    public static void setup() {
        Awaitility.setDefaultTimeout(ONE_MINUTE);
        Awaitility.pollExecutorService(executor);
    }

    @AfterClass
    public static void teardown() {
        shutdownAndAwaitTermination(executor, 500, MILLISECONDS);
    }

    public static void main(String[] args) throws Exception {
        ClusterTest.setup();
        ClusterTest test = new ClusterTest();
        Long randomSeed = Long.parseLong(args[0]);
        Integer numReplicas = Integer.parseInt(args[1]);
        test.testCanReachConcensusWithNReplicas(numReplicas, randomSeed);
        ClusterTest.teardown();
    }

    @Property()
    public void testCanReachConcensusWithNReplicas(
        @InRange(minInt = 2, maxInt = 25) int numReplicas,
        long randomSeed) throws Exception {

        Random random = new Random(randomSeed);

        CountingStateMachine stateMachine = spy(new CountingStateMachine(numReplicas));
        ChannelCluster cluster = ChannelCluster.createCluster(numReplicas, stateMachine, random, executor);
        cluster.startAsync().awaitRunning();

        await().until(() -> cluster.getLeader().isPresent());

        CompletableFuture<Object> commitFuture = cluster.send(MESSAGE);
        await().until(commitFuture::isDone);
        assertEquals(MESSAGE, commitFuture.get());

        assertThat(thereIsOneLeader(cluster.services()));
        assertThat(everyoneElseIsAFollower(cluster.services()));

        await().until(allReplicasHaveReceivedMessage(stateMachine));
        assertThat(allReplicasHaveReceivedMessage(stateMachine));
        verify(stateMachine, times(numReplicas)).applyOperation(MESSAGE);

        cluster.stopAsync().awaitTerminated();
    }

    static class CountingStateMachine implements StateMachine {
        private int numReplicas;

        CountingStateMachine(int numReplicas) {
            this.numReplicas = numReplicas;
        }

        synchronized int getNumReplicas() {
            return numReplicas;
        }

        @Override
        public synchronized Object applyOperation(@Nonnull ByteBuffer entry) {
            numReplicas--;
            return entry;
        }
    }
}
