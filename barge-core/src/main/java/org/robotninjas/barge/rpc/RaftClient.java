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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.RpcCallback;
import org.apache.commons.pool.ObjectPool;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.protobuf.netty.client.ClientController;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static org.robotninjas.barge.proto.RaftProto.*;

//TODO write a protoc code generator for this bullshit
@Immutable
class RaftClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);
  private static final long DEFAULT_TIMEOUT = 2000;

  private final ObjectPool<NettyRpcChannel> channelPool;

  RaftClient(@Nonnull ObjectPool<NettyRpcChannel> channelPool) {
    this.channelPool = checkNotNull(channelPool);
  }

  @Nonnull
  public ListenableFuture<RequestVoteResponse> requestVote(@Nonnull RequestVote request) {

    checkNotNull(request);

    NettyRpcChannel channel = null;
    try {

      channel = channelPool.borrowObject();
      RaftProto.RaftService.Stub stub = RaftProto.RaftService.newStub(channel);
      ClientController controller = new ClientController(channel);
      controller.setTimeout(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
      RpcHandlerFuture<RequestVoteResponse> responseHandler =
        new RpcHandlerFuture<RequestVoteResponse>(controller);
      stub.requestVote(controller, request, responseHandler);
      channelPool.returnObject(channel);
      return responseHandler;

    } catch (Exception e) {

      try {
        channelPool.invalidateObject(channel);
      } catch (Exception e1) {
      }
      channel = null;
      return immediateFailedFuture(e);

    } finally {

      try {
        if (null != channel) {
          channelPool.returnObject(channel);
        }
      } catch (Exception e) {
        // ignored
      }

    }

  }

  @Nonnull
  public ListenableFuture<AppendEntriesResponse> appendEntries(@Nonnull AppendEntries request) {

    checkNotNull(request);

    NettyRpcChannel channel = null;
    try {

      channel = channelPool.borrowObject();
      RaftProto.RaftService.Stub stub = RaftProto.RaftService.newStub(channel);
      ClientController controller = new ClientController(channel);
      controller.setTimeout(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
      RpcHandlerFuture<AppendEntriesResponse> responseHandler =
        new RpcHandlerFuture<AppendEntriesResponse>(controller);
      stub.appendEntries(controller, request, responseHandler);
      channelPool.returnObject(channel);
      return responseHandler;

    } catch (Exception e) {

      try {
        channelPool.invalidateObject(channel);
      } catch (Exception e1) {
      }
      channel = null;
      return immediateFailedFuture(e);

    } finally {

      try {
        if (null != channel) {
          channelPool.returnObject(channel);
        }
      } catch (Exception e) {
        // ignored
      }

    }

  }

  @Immutable
  private static class RpcHandlerFuture<T> extends AbstractFuture<T> implements RpcCallback<T> {

    @Nonnull
    private final ClientController controller;

    private RpcHandlerFuture(@Nonnull ClientController controller) {
      checkNotNull(controller);
      this.controller = controller;
    }

    @Override
    public void run(@Nullable T parameter) {

      if (isCancelled()) {
        return;
      }

      if (null == parameter) {
        setException(new RaftException(controller.errorText()));
      } else {
        set(parameter);
      }

    }

  }

  static final class RequestVoteCommand implements AsyncFunction<RequestVote, RequestVoteResponse> {

    private final NettyRpcChannel channel;

    RequestVoteCommand(NettyRpcChannel channel) {
      this.channel = channel;
    }

    @Override
    public ListenableFuture<RequestVoteResponse> apply(RequestVote request) throws Exception {
      RaftProto.RaftService.Stub stub = RaftProto.RaftService.newStub(channel);
      ClientController controller = new ClientController(channel);
      RpcHandlerFuture<RequestVoteResponse> handler =
        new RpcHandlerFuture<RequestVoteResponse>(controller);
      stub.requestVote(controller, request, handler);
      return handler;
    }

    public static RequestVoteCommand requestVote(NettyRpcChannel channel) {
      return new RequestVoteCommand(channel);
    }

  }

  static final class AppendEntriesCommand implements AsyncFunction<AppendEntries, AppendEntriesResponse> {

    private final NettyRpcChannel channel;

    AppendEntriesCommand(NettyRpcChannel channel) {
      this.channel = channel;
    }

    @Override
    public ListenableFuture<AppendEntriesResponse> apply(AppendEntries request) throws Exception {
      RaftProto.RaftService.Stub stub = RaftProto.RaftService.newStub(channel);
      ClientController controller = new ClientController(channel);
      RpcHandlerFuture<AppendEntriesResponse> handler =
        new RpcHandlerFuture<AppendEntriesResponse>(controller);
      stub.appendEntries(controller, request, handler);
      return handler;
    }

    public static AppendEntriesCommand appendEntries(NettyRpcChannel channel) {
      return new AppendEntriesCommand(channel);
    }
  }

}
