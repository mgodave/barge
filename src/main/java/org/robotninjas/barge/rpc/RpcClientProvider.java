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

import com.google.common.cache.*;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.robotninjas.barge.Replica;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public class RpcClientProvider
  extends CacheLoader<Replica, ObjectPool<NettyRpcChannel>>
  implements RemovalListener<Replica, ObjectPool<NettyRpcChannel>> {

  private final RpcClient client;
  private final LoadingCache<Replica, ObjectPool<NettyRpcChannel>> pools =
    CacheBuilder.newBuilder()
      .expireAfterAccess(10, TimeUnit.SECONDS)
      .removalListener(this)
      .build(this);

  @Inject
  public RpcClientProvider(RpcClient client) {
    this.client = client;
  }

  public RaftClient get(Replica replica) {
    return new RaftClient(pools.getUnchecked(replica));
  }

  @Override
  public void onRemoval(RemovalNotification<Replica, ObjectPool<NettyRpcChannel>> notification) {
    try {
      notification.getValue().close();
    } catch (Exception e) {

    }
  }

  @Override
  public ObjectPool<NettyRpcChannel> load(Replica key) throws Exception {
    GenericObjectPool<NettyRpcChannel> pool =
      new GenericObjectPool<NettyRpcChannel>(
        new RpcChannelFactory(client, key.address()));
    pool.setMaxActive(1);
    pool.setTestOnBorrow(true);
    pool.setTestWhileIdle(true);
    return pool;
  }
}
