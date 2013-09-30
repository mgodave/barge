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

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import org.robotninjas.barge.annotations.*;
import org.robotninjas.barge.log.LogModule;
import org.robotninjas.barge.rpc.RpcModule;
import org.robotninjas.barge.state.StateModule;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

@Immutable
@ThreadSafe
public class RaftModule extends PrivateModule {

  private static final long DEFAULT_TIMEOUT = 225;

  private final Replica local;
  private final long timeout;
  private final File logDir;
  private final List<Replica> members;

  public RaftModule(@Nonnull Replica local, @Nonnull List<Replica> members, @Nonnegative long timeout, @Nonnull File logDir) {
    this.local = checkNotNull(local);
    checkArgument(timeout > 0);
    this.timeout = timeout;
    this.logDir = checkNotNull(logDir);
    this.members = checkNotNull(members);
  }

  public RaftModule(@Nonnull Replica local, @Nonnull List<Replica> members, @Nonnull File logDir) {
    this(local, members, DEFAULT_TIMEOUT, logDir);
  }

  @Override
  protected void configure() {

    install(new StateModule());
    install(new RpcModule(local.address()));
    install(new LogModule(logDir));

    bind(Replica.class)
      .annotatedWith(LocalReplicaInfo.class)
      .toInstance(local);

    bind(new TypeLiteral<List<Replica>>() {})
      .annotatedWith(ClusterMembers.class)
      .toInstance(members);

    bind(Long.class)
      .annotatedWith(ElectionTimeout.class)
      .toInstance(timeout);

    bind(ExecutorService.class)
      .annotatedWith(LogListenerExecutor.class)
      .toInstance(newSingleThreadExecutor());

    bind(RaftService.class).to(DefaultRaftService.class);
    expose(RaftService.class);

    ListeningExecutorService stateMachineExecutor =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    bind(ListeningExecutorService.class)
      .annotatedWith(StateMachineExecutor.class)
      .toInstance(stateMachineExecutor);

    bind(EventBus.class).toInstance(new EventBus());

  }
}
