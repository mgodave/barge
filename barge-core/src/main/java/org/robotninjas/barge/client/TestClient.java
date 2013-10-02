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
package org.robotninjas.barge.client;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.proto.ClientProto;
import org.robotninjas.protobuf.netty.client.ClientController;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.robotninjas.barge.client.TestClient.Predicates.NotCommitted;
import static org.robotninjas.barge.proto.ClientProto.*;

public class TestClient {

  public static void main(String... args) throws InterruptedException {

    NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    EventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(1);
    final RpcClient client = new RpcClient(eventLoopGroup, eventExecutorGroup);

    GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
    config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_FAIL;
    config.maxActive = 1;
    config.testOnBorrow = true;
    config.testOnReturn = true;

    final GenericKeyedObjectPool<Object, NettyRpcChannel> channelPool =
      new GenericKeyedObjectPool<Object, NettyRpcChannel>(
        new PoolableChannelFactory(client), config);

    Retryer<CommitOperationResponse> retryer =
      RetryerBuilder.<CommitOperationResponse>newBuilder()
        .retryIfException()
        .retryIfRuntimeException()
        .withWaitStrategy(exponentialWait(2, SECONDS))
        .withStopStrategy(stopAfterAttempt(10))
        .retryIfResult(NotCommitted)
        .build();

    long seq = 0;
    String leader = "localhost:10000";
    for (; ; ) {

      try {

        ByteBuffer message = ByteBuffer.allocate(8);
        message.putLong(seq).rewind();

        CommitOperation.Builder operation =
          CommitOperation.newBuilder()
            .setClientId("client1")
            .setOp(ByteString.copyFrom(message))
            .setSequence(seq++);

        ObjectPool<NettyRpcChannel> pool = PoolUtils.adapt(channelPool, leader);
        CommitOperationCommand command = new CommitOperationCommand(operation, pool);
        CommitOperationResponse response = retryer.call(command);

        if (response.hasRedirect()) {
          System.out.println("Redirect " + response.getRedirect().getLeaderId());
          //leader = response.getRedirect().getLeaderId();
        } else if (response.hasCommitted()) {
          System.out.println("Committed " + response.getCommitted());
        } else if (response.hasErrorCode()) {
          System.out.println("ErrorCode " + response.getErrorCode());
        }

      } catch (ExecutionException e) {
        System.out.println(e.getMessage());
      } catch (RetryException e) {
        System.out.println("Max retries");
      }

      //Thread.sleep(50);

    }

  }

  static final class FutureCallback
    extends AbstractFuture<CommitOperationResponse>
    implements RpcCallback<CommitOperationResponse> {

    private final RpcController controller;

    private FutureCallback(RpcController controller) {
      this.controller = controller;
    }

    @Override
    public void run(CommitOperationResponse value) {
      if (null == value) {
        setException(new RaftException(controller.errorText()));
      } else {
        set(value);
      }
    }

  }

  @Immutable
  static final class CommitOperationCommand implements Callable<CommitOperationResponse> {

    private final CommitOperation.Builder operation;
    private final ObjectPool<NettyRpcChannel> channelPool;

    private CommitOperationCommand(CommitOperation.Builder operation, ObjectPool<NettyRpcChannel> channelPool) {
      this.operation = operation;
      this.channelPool = channelPool;
    }

    @Override
    public CommitOperationResponse call() throws Exception {

      NettyRpcChannel channel = null;
      try {

        System.out.println("Sending " + operation.getSequence());

        channel = channelPool.borrowObject();
        ClientController controller = new ClientController(channel);
        ClientService.Stub stub = ClientService.newStub(channel);

        CommitOperation request = operation.build();

        FutureCallback callback = new FutureCallback(controller);
        stub.commitOperation(controller, request, callback);

        return callback.get(10, SECONDS);

      } catch (Exception e) {

        System.out.println(e.getMessage());
        throw e;

      } finally {

        if (null != channel) {
          channelPool.returnObject(channel);
        }

      }

    }
  }

  @Immutable
  static enum Predicates implements Predicate<ClientProto.CommitOperationResponse> {

    NotCommitted;

    @Override
    public boolean apply(@Nullable CommitOperationResponse input) {
      return !input.getCommitted();
    }

  }

  @Immutable
  static final class PoolableChannelFactory extends BaseKeyedPoolableObjectFactory<Object, NettyRpcChannel> {

    private final RpcClient client;

    PoolableChannelFactory(RpcClient client) {
      this.client = client;
    }

    @Override
    public NettyRpcChannel makeObject(Object input) throws Exception {
      Preconditions.checkArgument(input instanceof String);
      String key = (String) input;
      String[] parts = key.split(":");
      InetAddress addr = InetAddress.getByName(parts[0]);
      int port = Integer.parseInt(parts[1]);
      return client.connect(new InetSocketAddress(addr, port));
    }
  }

}
