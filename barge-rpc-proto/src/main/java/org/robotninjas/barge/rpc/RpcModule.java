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

package org.robotninjas.barge.rpc;

import com.google.inject.PrivateModule;
import io.netty.channel.nio.NioEventLoopGroup;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.robotninjas.protobuf.netty.server.RpcServer;

import javax.annotation.Nonnull;
import java.net.SocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;

public class RpcModule extends PrivateModule {

  private final SocketAddress saddr;
  private final NioEventLoopGroup eventLoopGroup;

  public RpcModule(@Nonnull SocketAddress saddr, NioEventLoopGroup eventLoopGroup) {
    this.saddr = checkNotNull(saddr);
    this.eventLoopGroup = checkNotNull(eventLoopGroup);
  }

  @Override
  protected void configure() {

    bind(NioEventLoopGroup.class)
        .toInstance(eventLoopGroup);

    RpcServer rpcServer = new RpcServer(eventLoopGroup, saddr);

    bind(RpcServer.class)
        .toInstance(rpcServer);
    expose(RpcServer.class);

    bind(RaftClientProvider.class)
        .to(ProtoRpcRaftClientProvider.class)
        .asEagerSingleton();

    expose(RaftClientProvider.class);
    
    bind(RpcClient.class)
        .toInstance(new RpcClient(eventLoopGroup));
  }

}
