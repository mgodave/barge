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

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.File;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.barge.state.AbstractListenersModule;

public class ChannelRaftModule extends PrivateModule {
    private static final BatchExecutor batchExecutor = new BatchExecutor();

    private final Executor raftExecutor;
    private final Executor clientExecutor;
    private final ClusterConfig config;
    private final File logDir;
    private final StateMachine stateMachine;
    private final long timeout;
    private final Random random;
    private final ScheduledExecutorService scheduledExecutor;

    private ChannelRaftModule(Builder builder) {
        this.config = builder.config;
        this.logDir = builder.logDir;
        this.stateMachine = builder.stateMachine;
        this.timeout = builder.timeout;
        this.raftExecutor = builder.raftExecutor;
        this.clientExecutor = builder.clientExecutor;
        this.random = builder.random;
        this.scheduledExecutor = builder.scheduledExecutor;
    }

    @Provides
    @Singleton
    private Fiber provideFiber() {
        PoolFiberFactory fiberFactory = new PoolFiberFactory(clientExecutor, scheduledExecutor);
        Fiber fiber = fiberFactory.create(batchExecutor);
        fiber.start();
        return fiber;
    }

    @Override
    protected void configure() {
        install(new AbstractListenersModule() {
            @Override
            protected void configureListeners() {
            }
        });

        install(RaftCoreModule.builder()
            .withTimeout(timeout)
            .withConfig(config)
            .withLogDir(logDir)
            .withStateMachine(stateMachine)
            .withExecutor(raftExecutor)
            .withScheduledExecutor(scheduledExecutor)
            .withRandom(random)
            .build());

        bind(RaftClientProvider.class)
            .to(ChannelClientProvider.class);
        expose(RaftClientProvider.class);

        bind(ChannelRaftService.class);
        expose(ChannelRaftService.class);
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder {
        private Executor raftExecutor;
        private Executor clientExecutor;
        private ClusterConfig config;
        private File logDir;
        private StateMachine stateMachine;
        private long timeout;
        private Random random;
        private ScheduledExecutorService scheduledExecutor;

        private Builder() {

        }

        public Builder withRaftExecutor(Executor raftExecutor) {
            this.raftExecutor = raftExecutor;
            return this;
        }

        public Builder withClientExecutor(Executor clientExecutor) {
            this.clientExecutor = clientExecutor;
            return this;
        }

        public Builder withConfig(ClusterConfig config) {
            this.config = config;
            return this;
        }

        public Builder withLogDir(File logDir) {
            this.logDir = logDir;
            return this;
        }

        public Builder withStateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withRandom(Random random) {
            this.random = random;
            return this;
        }

        public Builder withScheduledExecutor(ScheduledExecutorService scheduledExecutor) {
            this.scheduledExecutor = scheduledExecutor;
            return this;
        }

        public ChannelRaftModule build() {
            return new ChannelRaftModule(this);
        }
    }
}
