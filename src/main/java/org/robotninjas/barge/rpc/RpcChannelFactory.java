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

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;

import java.net.SocketAddress;

public class RpcChannelFactory extends BasePoolableObjectFactory<NettyRpcChannel> {

  private final RpcClient client;
  private final SocketAddress saddr;

  public RpcChannelFactory(RpcClient client, SocketAddress saddr) {
    this.client = client;
    this.saddr = saddr;
  }

  @Override
  public NettyRpcChannel makeObject() throws Exception {
    return client.connect(saddr);
  }

  @Override
  public void destroyObject(NettyRpcChannel obj) throws Exception {
    obj.close();
  }

  @Override
  public boolean validateObject(NettyRpcChannel obj) {
    return obj.isOpen();
  }
}
