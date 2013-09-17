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
