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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import net.javacrumbs.futureconverter.java8guava.FutureConverter;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.robotninjas.barge.NettyReplica;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
class RpcChannelFactory extends BaseKeyedPoolableObjectFactory<Object, CompletableFuture<NettyRpcChannel>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RpcChannelFactory.class);

  private final RpcClient client;

  public RpcChannelFactory(@Nonnull RpcClient client) {
    this.client = checkNotNull(client);
  }

  @Override
  public CompletableFuture<NettyRpcChannel> makeObject(Object key) throws Exception {
    Preconditions.checkArgument(key instanceof NettyReplica);
    NettyReplica replica = (NettyReplica) key;
    return FutureConverter.toCompletableFuture(client.connectAsync(replica.address()));
  }

  @Override
  public void destroyObject(Object key, CompletableFuture<NettyRpcChannel> obj) throws Exception {
    if (obj.isDone() && !obj.isCancelled()) {
      obj.get().close();
    } else {
      obj.cancel(false);
    }
  }

  @Override
  public boolean validateObject(Object key, CompletableFuture<NettyRpcChannel> obj) {
    return !obj.isDone() || (obj.isDone() && Futures.getUnchecked(obj).isOpen());
  }

}
