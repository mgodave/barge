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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.proto.ClientProto;
import org.robotninjas.protobuf.netty.client.ClientController;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.protobuf.netty.client.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.rholder.retry.StopStrategies.stopAfterAttempt;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.pool.impl.GenericObjectPool.DEFAULT_MAX_WAIT;
import static org.apache.commons.pool.impl.GenericObjectPool.WHEN_EXHAUSTED_FAIL;
import static org.robotninjas.barge.client.BargeClient.Predicates.NotCommitted;
import static org.robotninjas.barge.proto.ClientProto.*;

public class BargeClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(BargeClient.class);

  private final String id;
  private final AtomicInteger sequence;

  private final GenericKeyedObjectPool<Object, NettyRpcChannel> channels;

  private final RetryerBuilder<CommitOperationResponse> retryerBuilder =
    RetryerBuilder.<CommitOperationResponse>newBuilder()
      .retryIfException()
      .retryIfRuntimeException()
      .withWaitStrategy(exponentialWait(2, SECONDS))
      .withStopStrategy(stopAfterAttempt(10))
      .retryIfResult(NotCommitted);

  private Set<String> bootstrapNodes = Sets.newConcurrentHashSet();
  private String leader;

  public BargeClient(RpcClient rpcClient, String id, int sequence, String bootstrap) {

    this.sequence = new AtomicInteger(sequence);
    this.leader = checkNotNull(bootstrap);
    this.id = checkNotNull(id);

    PoolableChannelFactory objectFactory = new PoolableChannelFactory(rpcClient);
    this.channels = new GenericKeyedObjectPool<Object, NettyRpcChannel>(
      objectFactory, 1, WHEN_EXHAUSTED_FAIL, DEFAULT_MAX_WAIT, true, true);

  }

  public boolean commit(byte[] cmd) throws RaftException, RetryException {

    boolean redirect;
    try {

      CommitOperation.Builder operation =
        CommitOperation.newBuilder()
          .setClientId(ByteString.copyFromUtf8(id))
          .setOp(ByteString.copyFrom(cmd))
          .setSequence(sequence.incrementAndGet());

      do {

        ObjectPool<NettyRpcChannel> pool = PoolUtils.adapt(channels, leader);
        CommitOperationCommand command = new CommitOperationCommand(operation, pool);
        Retryer<CommitOperationResponse> retryer = retryerBuilder.build();
        CommitOperationResponse response = retryer.call(command);

        redirect = response.hasRedirect();

        bootstrapNodes = newHashSet(response.getClusterMembersList());

        if (response.hasRedirect()) {
          leader = response.getRedirect().getLeaderId();
          LOGGER.info("redirecting");
        } else if (response.hasCommitted()) {
          return response.hasCommitted();
        } else if (response.hasErrorCode()) {
          throw new RaftException(response.getErrorCode().toString());
        }

      } while (redirect);

    } catch (ExecutionException e) {
      throw new RaftException(e.getCause());
    }

    return false;

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
        channelPool.invalidateObject(channel);
        channel = null;
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
