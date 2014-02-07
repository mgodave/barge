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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.robotninjas.barge.Replica;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
class ProtoRpcRaftClientProvider implements RaftClientProvider {

  private static final GenericKeyedObjectPool.Config config;

  static {
    config = new GenericKeyedObjectPool.Config();
    config.maxActive = 1;
    config.maxIdle = -1;
    config.testOnBorrow = true;
    config.testOnReturn = true;
    config.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_FAIL;
  }

  private final KeyedObjectPool<Object, ListenableFuture<NettyRpcChannel>> connectionPools;

  @Inject
  public ProtoRpcRaftClientProvider(@Nonnull RpcClient client) {
    RpcChannelFactory channelFactory = new RpcChannelFactory(client);
    this.connectionPools = new GenericKeyedObjectPool(channelFactory, config);
  }

  @Nonnull
  public RaftClient get(@Nonnull Replica replica) {
    checkNotNull(replica);
    return new ProtoRpcRaftClient(PoolUtils.adapt(connectionPools, replica));
  }

}
