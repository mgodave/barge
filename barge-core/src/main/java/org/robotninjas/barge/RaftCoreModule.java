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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.PrivateModule;
import org.robotninjas.barge.log.LogModule;
import org.robotninjas.barge.rpc.Client;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.StateModule;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
class RaftCoreModule extends PrivateModule {

  private static final long DEFAULT_TIMEOUT = 225;

  private long timeout = DEFAULT_TIMEOUT;
  private final ClusterConfig config;
  private final File logDir;
  private final StateMachine stateMachine;
  private Optional<ListeningExecutorService> stateMachineExecutor = Optional.absent();

  public RaftCoreModule(@Nonnull ClusterConfig config, @Nonnull File logDir, @Nonnull StateMachine stateMachine) {
    this.config = checkNotNull(config);
    checkArgument(timeout > 0);
    this.logDir = checkNotNull(logDir);
    this.stateMachine = checkNotNull(stateMachine);
  }

  @Override
  protected void configure() {

    install(new StateModule(timeout));
    expose(Raft.class);

    final ListeningExecutorService executor;
    if (stateMachineExecutor.isPresent()) {
      executor = stateMachineExecutor.get();
    } else {
      executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          executor.shutdownNow();
        }
      });
    }
    install(new LogModule(logDir, stateMachine, executor));

    bind(ClusterConfig.class)
      .toInstance(config);

    bind(Client.class).asEagerSingleton();
    expose(Client.class);
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

}
