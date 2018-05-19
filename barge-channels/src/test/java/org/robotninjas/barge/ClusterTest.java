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

import static com.github.rholder.retry.StopStrategies.neverStop;
import static com.github.rholder.retry.WaitStrategies.fibonacciWait;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.pholser.junit.quickcheck.Mode.EXHAUSTIVE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_MINUTE;
import static org.awaitility.Duration.TEN_SECONDS;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.stream.IntStream;
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
    private static final ThreadFactory WORK_THREAD_FACTORY =
        new ThreadFactoryBuilder()
            .setNameFormat("raft-pool-%d")
            .setDaemon(true).build();

    private static final ThreadFactory CLIENT_THREAD_FACTORY =
        new ThreadFactoryBuilder()
            .setNameFormat("client-pool-%d")
            .setDaemon(true).build();

    private static final ThreadFactory SCHEDULER_THREAD_FACTORY =
        new ThreadFactoryBuilder()
            .setNameFormat("global-scheduler-%d")
            .setDaemon(true).build();

    private static final ExecutorService WORK_EXECUTOR = (ExecutorService) newSingleThreadExecutor(WORK_THREAD_FACTORY);
    private static final ExecutorService CLIENT_EXECUTOR = (ExecutorService) newSingleThreadExecutor(CLIENT_THREAD_FACTORY);
    private static final ScheduledExecutorService SCHEDULER = newSingleThreadScheduledExecutor(SCHEDULER_THREAD_FACTORY);
    private static final ByteBuffer MESSAGE = ByteBuffer.wrap("Hi there".getBytes());
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTest.class);
    private static Retryer<Object> RETRYER = RetryerBuilder.newBuilder()
        .retryIfException()
        .retryIfRuntimeException()
        .withWaitStrategy(fibonacciWait(10, SECONDS))
        .withStopStrategy(neverStop())
        .build();

    private static void stopCluster(List<ChannelRaftService> services) {
        services.stream()
            .map(Service::stopAsync)
            .forEach(Service::awaitTerminated);
    }

    private static void startCluster(List<ChannelRaftService> services) {
        services.stream()
            .map(Service::startAsync)
            .forEach(Service::awaitRunning);
    }

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

    private static Optional<ChannelRaftService> getLeader(List<ChannelRaftService> cluster) {
        return cluster.stream().filter(s -> s.getState() == Raft.StateType.LEADER).findAny();
    }

    private static Callable<Boolean> allReplicasHaveReceivedMessage(CountingStateMachine stateMachine) {
        return () -> stateMachine.getNumReplicas() == 0;
    }

    private static List<ChannelRaftService> createCluster(int numReplicas, StateMachine stateMachine, Random randomness) {
        ChannelReplicaFactory replicaFactory = new ChannelReplicaFactory();

        Set<Replica> replicas = IntStream.range(0, numReplicas)
            .mapToObj(ignore -> replicaFactory.newReplica())
            .collect(toSet());

        return replicas.stream().map(local -> {
            ClusterConfig config = replicaFactory.newClusterConfig(
                local,
                Sets.difference(replicas, newHashSet(local))
            );

            return ChannelRaftService
                .newBuilder(config)
                .raftExecutor(WORK_EXECUTOR)
                .clientExecutor(CLIENT_EXECUTOR)
                .scheduledExecutor(SCHEDULER)
                .random(randomness)
                .build(stateMachine);

        }).collect(toList());
    }

    private static CompletableFuture<Object> sendMessageToCluster(List<ChannelRaftService> cluster, ByteBuffer message) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return RETRYER.call(() -> {
                    ChannelRaftService leader = getLeader(cluster)
                        .orElseThrow(NoLeaderException::new);
                    LOGGER.info("Committing to leader, {}", leader.getLocal());
                    return leader.commitAsync(message.array()).get();
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @BeforeClass
    public static void setup() {
        Awaitility.setDefaultTimeout(ONE_MINUTE);
    }

    @AfterClass
    public static void cleanup() {
        shutdownAndAwaitTermination(WORK_EXECUTOR, 5, SECONDS);
        shutdownAndAwaitTermination(CLIENT_EXECUTOR, 5, SECONDS);
        shutdownAndAwaitTermination(SCHEDULER, 5, SECONDS);
    }

    @Property
    public void testCanReachConcensusWithNReplicas(
        @InRange(minInt = 3, maxInt = 25) int numReplicas,
        @From(RandomGenerator.class) Random randomness) throws Exception {
        LOGGER.info("Running with {} replicas", numReplicas);

        CountingStateMachine stateMachine = spy(new CountingStateMachine(numReplicas));
        List<ChannelRaftService> cluster = createCluster(numReplicas, stateMachine, randomness);
        startCluster(cluster);
        Thread.sleep(1000);

        CompletableFuture<Object> commitFuture = sendMessageToCluster(cluster, MESSAGE);
        await().until(() -> commitFuture.isDone() && !commitFuture.isCompletedExceptionally());
        commitFuture.get();

        assertThat(thereIsOneLeader(cluster));
        assertThat(everyoneElseIsAFollower(cluster));

        await().until(allReplicasHaveReceivedMessage(stateMachine));
        assertThat(allReplicasHaveReceivedMessage(stateMachine));
        verify(stateMachine, times(numReplicas)).applyOperation(MESSAGE);

        stopCluster(cluster);
    }

    static class CountingStateMachine implements StateMachine {
        private int numReplicas;

        CountingStateMachine(int numReplicas) {
            this.numReplicas = numReplicas;
        }

        public synchronized int getNumReplicas() {
            return numReplicas;
        }

        @Override
        public synchronized Object applyOperation(@Nonnull ByteBuffer entry) {
            numReplicas--;
            return null;
        }
    }

    public static class RandomGenerator extends Generator<Random> {
        public RandomGenerator() {
            super(Random.class);
        }

        @Override
        public Random generate(
            SourceOfRandomness sourceOfRandomness,
            GenerationStatus generationStatus) {

            return sourceOfRandomness.toJDKRandom();
        }
    }

    private static class NoLeaderException extends Exception {
        public NoLeaderException() {
            super();
        }
    }
}
