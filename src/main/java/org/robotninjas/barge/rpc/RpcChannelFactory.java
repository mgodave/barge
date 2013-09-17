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
