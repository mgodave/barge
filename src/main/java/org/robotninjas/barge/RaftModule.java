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

import com.google.inject.PrivateModule;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.LocalReplicaInfo;
import org.robotninjas.barge.context.ContextModule;
import org.robotninjas.barge.log.LogModule;
import org.robotninjas.barge.rpc.RpcModule;
import org.robotninjas.barge.state.StateModule;

import java.net.InetSocketAddress;

public class RaftModule extends PrivateModule {

  private static final long DEFAULT_TIMEOUT = 1000;

  private final InetSocketAddress saddr;
  private final long timeout;

  public RaftModule(InetSocketAddress saddr, long timeout) {
    this.saddr = saddr;
    this.timeout = timeout;
  }

  public RaftModule(InetSocketAddress saddr) {
    this(saddr, DEFAULT_TIMEOUT);
  }

  @Override
  protected void configure() {
    install(new StateModule());
    install(new RpcModule(saddr));
    install(new LogModule());
    install(new ContextModule());
    bind(Replica.class).annotatedWith(LocalReplicaInfo.class).toInstance(new Replica(saddr));
    bind(RaftService.class).to(DefaultRaftService.class);
    expose(RaftService.class);
    bind(Long.class).annotatedWith(ElectionTimeout.class).toInstance(timeout);
  }
}
