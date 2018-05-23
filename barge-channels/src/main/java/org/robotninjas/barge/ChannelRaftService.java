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

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractService;
import com.google.inject.Guice;
import java.io.File;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import org.jetlang.core.Disposable;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.Raft.StateType;
import org.robotninjas.barge.state.StateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelRaftService extends AbstractService implements RaftService {
    private static Logger LOGGER = LoggerFactory.getLogger(ChannelRaftService.class);

    private final Fiber fiber;
    private final ClusterConfig config;
    private final Raft ctx;

    private Disposable appendEntriesSubscription;
    private Disposable requestVoteSubscription;

    @Inject
    public ChannelRaftService(ClusterConfig config, Raft ctx, Fiber fiber) {
        this.config = config;
        this.ctx = ctx;
        this.fiber = fiber;
    }

    public static Builder newBuilder(ClusterConfig config) {
        return new Builder(config);
    }

    @Override
    protected void doStart() {
        ChannelReplica local = (ChannelReplica) config.local();
        appendEntriesSubscription = local.getAppendEntriesChannel()
            .subscribe(fiber, request ->
                ctx.appendEntries(request.getRequest())
                    .thenAccept(request::reply)
                    .exceptionally(e -> {
                        LOGGER.warn("Error sending AppendEntries", e);
                        return null;
                    })
            );

        requestVoteSubscription = local.getRequestVoteChannel()
            .subscribe(fiber, request ->
                ctx.requestVote(request.getRequest())
                    .thenAccept(request::reply)
                    .exceptionally(e -> {
                        LOGGER.warn("Error sending RequestVote", e);
                        return null;
                    })
            );

        CompletableFuture<StateType> init = ctx.init();
        init.thenRun(this::notifyStarted);
        init.exceptionally((t) -> {
            notifyFailed(t);
            return StateType.STOPPED;
        });
    }

    @Override
    protected void doStop() {
        if (null != appendEntriesSubscription) {
            appendEntriesSubscription.dispose();
        }

        if (null != requestVoteSubscription) {
            requestVoteSubscription.dispose();
        }

        ctx.stop();

        notifyStopped();
    }

    public void addTransitionListener(StateTransitionListener listener) {
        ctx.addTransitionListener(listener);
    }

    public StateType getState() {
        return ctx.type();
    }

    public Replica getLocal() {
        return config.local();
    }

    @Override
    public CompletableFuture<Object> commitAsync(byte[] operation) {
        return ctx.commitOperation(operation);
    }

    public static class Builder {

        private static long TIMEOUT = 150;

        private final ClusterConfig config;
        private File logDir = Files.createTempDir();
        private long timeout = TIMEOUT;
        private StateTransitionListener listener;
        private Executor raftExecutor;
        private Executor clientExecutor;
        private Random random;
        private ScheduledExecutorService scheduledExecutor;

        private Builder(ClusterConfig config) {
            this.config = config;
        }

        public Builder timeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder logDir(File logDir) {
            this.logDir = logDir;
            return this;
        }

        public Builder random(Random random) {
            this.random = random;
            return this;
        }

        public Builder raftExecutor(Executor executor) {
            this.raftExecutor = executor;
            return this;
        }

        public Builder clientExecutor(Executor executor) {
            this.clientExecutor = executor;
            return this;
        }

        public Builder scheduledExecutor(ScheduledExecutorService scheduledExecutor) {
            this.scheduledExecutor = scheduledExecutor;
            return this;
        }

        public ChannelRaftService build(StateMachine stateMachine) {
            if (raftExecutor == null) {
                raftExecutor = newSingleThreadExecutor();
            }

            if (clientExecutor == null) {
                clientExecutor = newSingleThreadExecutor();
            }

            ChannelRaftService channelRaftService = Guice.createInjector(
                ChannelRaftModule.newBuilder()
                    .withConfig(config)
                    .withLogDir(logDir)
                    .withStateMachine(stateMachine)
                    .withTimeout(timeout)
                    .withRaftExecutor(raftExecutor)
                    .withClientExecutor(clientExecutor)
                    .withRandom(random)
                    .withScheduledExecutor(scheduledExecutor)
                    .build()
            ).getInstance(ChannelRaftService.class);

            if (listener != null) {
                channelRaftService.addTransitionListener(listener);
            }

            return channelRaftService;
        }

        public Builder transitionListener(StateTransitionListener listener) {
            this.listener = listener;
            return this;
        }
    }
}
