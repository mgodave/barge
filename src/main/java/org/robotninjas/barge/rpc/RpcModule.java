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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.PrivateModule;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.robotninjas.barge.annotations.RaftExecutor;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.robotninjas.protobuf.netty.server.RpcServer;

import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

public class RpcModule extends PrivateModule {

  private static final int NUM_THREADS = 1;
  private final SocketAddress saddr;

  public RpcModule(SocketAddress saddr) {
    this.saddr = saddr;
  }

  @Override
  protected void configure() {

    ThreadFactoryBuilder factoryBuilder =
      new ThreadFactoryBuilder()
        .setNameFormat("Raft Thread")
        .setDaemon(true);

    final DefaultEventExecutorGroup eventExecutor = new DefaultEventExecutorGroup(1, factoryBuilder.build());
    bind(EventExecutorGroup.class).toInstance(eventExecutor);

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        eventExecutor.shutdownGracefully();
      }
    }));

    ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(eventExecutor);
    bind(ListeningExecutorService.class).annotatedWith(RaftExecutor.class).toInstance(listeningExecutorService);
    expose(ListeningExecutorService.class).annotatedWith(RaftExecutor.class);

    ListeningScheduledExecutorService listeningScheduler = MoreExecutors.listeningDecorator(eventExecutor);
    bind(ScheduledExecutorService.class).annotatedWith(RaftScheduler.class).toInstance(listeningScheduler);
    expose(ScheduledExecutorService.class).annotatedWith(RaftScheduler.class);

    NioEventLoopGroup eventLoop = new NioEventLoopGroup(NUM_THREADS);
    bind(NioEventLoopGroup.class).toInstance(eventLoop);

    RpcServer rpcServer = new RpcServer(eventLoop, eventExecutor, saddr);
    bind(RpcServer.class).toInstance(rpcServer);
    expose(RpcServer.class);

    bind(RpcClientProvider.class);
    expose(RpcClientProvider.class);

    bind(RpcClient.class);
    expose(RpcClient.class);
  }

}
