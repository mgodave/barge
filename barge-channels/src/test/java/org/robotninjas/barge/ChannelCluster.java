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

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.nurkiewicz.asyncretry.AsyncRetryExecutor;
import com.nurkiewicz.asyncretry.RetryExecutor;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.robotninjas.barge.state.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelCluster extends AbstractService implements Cluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelCluster.class);

    private final List<ChannelRaftService> services;
    private final RetryExecutor asyncRetryer;

    private ChannelCluster(
        List<ChannelRaftService> services,
        ScheduledExecutorService scheduler) {

        this.services = services;
        this.asyncRetryer = new AsyncRetryExecutor(scheduler)
            .withExponentialBackoff(500, 2)
            .withMaxDelay(10_000)
            .retryInfinitely();
    }

    static ChannelCluster createCluster(
        int numReplicas,
        StateMachine stateMachine,
        Random randomness,
        ScheduledExecutorService executor) {

        ChannelReplicaFactory replicaFactory = new ChannelReplicaFactory();

        Set<Replica> replicas = IntStream.range(0, numReplicas)
            .mapToObj(ignore -> replicaFactory.newReplica())
            .collect(Collectors.toCollection(LinkedHashSet::new));

        List<ChannelRaftService> services = replicas.stream().map(local -> {
            LOGGER.debug("Creating service for {}", local);
            ClusterConfig config = replicaFactory.newClusterConfig(
                local,
                Sets.difference(replicas, newHashSet(local))
            );

            return ChannelRaftService
                .newBuilder(config)
                .raftExecutor(executor)
                .clientExecutor(executor)
                .scheduledExecutor(executor)
                .random(randomness)
                .build(stateMachine);

        }).collect(toList());

        return new ChannelCluster(services, executor);
    }

    @Override
    protected void doStart() {
        for (ChannelRaftService service : services) {
            service.startAsync().awaitRunning();
        }
        notifyStarted();
    }

    @Override
    protected void doStop() {
        for (ChannelRaftService service : services) {
            service.stopAsync().awaitTerminated();
        }
        notifyStopped();
    }

    @Override
    public CompletableFuture<Object> send(ByteBuffer message) {
        return asyncRetryer.getFutureWithRetry(retryContext -> {
            ChannelRaftService leader = getLeader()
                .orElseThrow(NoLeaderException::new);
            LOGGER.info("Committing to leader, {}", leader.getLocal());
            return leader.commitAsync(message.array());
        });
    }

    @Override
    public Optional<ChannelRaftService> getLeader() {
        return services.stream().filter(s -> s.getState() == Raft.StateType.LEADER).findAny();
    }

    @Override
    public List<ChannelRaftService> services() {
        return Collections.unmodifiableList(services);
    }

}
