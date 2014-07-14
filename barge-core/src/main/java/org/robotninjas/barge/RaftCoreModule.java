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

package org.robotninjas.barge;

import com.google.common.base.Optional;
import com.google.inject.PrivateModule;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;
import org.robotninjas.barge.log.LogModule;
import org.robotninjas.barge.rpc.Client;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.StateModule;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newCachedThreadPool;


@Immutable
public class RaftCoreModule extends PrivateModule {

  private static final long DEFAULT_TIMEOUT = 225;

  private final long timeout;
  private final ClusterConfig config;
  private final File logDir;
  private final StateMachine stateMachine;
  private final Executor executor;

  private RaftCoreModule(Builder builder) {
    this.config = builder.config.get();
    this.timeout = builder.timeout;
    this.logDir = builder.logDir.get();
    this.stateMachine = builder.stateMachine.get();
    this.executor = builder.executor.or(newCachedThreadPool());
  }

  @Override
  protected void configure() {
    install(new StateModule(timeout));

    PoolFiberFactory fiberFactory = new PoolFiberFactory(executor);

    Fiber raftFiber = fiberFactory.create(new BatchExecutor());
    raftFiber.start();
    bind(Fiber.class).annotatedWith(RaftExecutor.class).toInstance(raftFiber);

    Fiber stateMachineFiber = fiberFactory.create(new BatchExecutor());
    stateMachineFiber.start();

    install(new LogModule(logDir, stateMachine, stateMachineFiber));

    bind(ClusterConfig.class).toInstance(config);

    bind(Client.class).asEagerSingleton();

    expose(Raft.class);
    expose(ClusterConfig.class);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private long timeout = DEFAULT_TIMEOUT;
    private Optional<Executor> executor = Optional.absent();
    private Optional<ClusterConfig> config = Optional.absent();
    private Optional<StateMachine> stateMachine = Optional.absent();
    private Optional<File> logDir = Optional.absent();

    private Builder() {

    }

    public Builder withTimeout(long timeout) {
      this.timeout = timeout;

      return this;
    }

    public Builder withConfig(ClusterConfig config) {
      this.config = Optional.of(config);

      return this;
    }

    public Builder withStateMachine(StateMachine stateMachine) {
      this.stateMachine = Optional.of(stateMachine);

      return this;
    }

    public Builder withLogDir(File logDir) {
      this.logDir = Optional.of(logDir);

      return this;
    }

    public RaftCoreModule build() {
      checkState(config.isPresent());
      checkState(stateMachine.isPresent());
      checkState(logDir.isPresent());
      checkArgument(timeout > 0);

      return new RaftCoreModule(this);
    }

  }

}
