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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.pool.ObjectPool;
import org.robotninjas.barge.ProtoUtils;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.protobuf.netty.client.ClientController;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;

//TODO write a protoc code generator for this bullshit
@Immutable
class ProtoRpcRaftClient implements RaftClient {

  private static final long DEFAULT_TIMEOUT = 2000;

  private final ObjectPool<CompletableFuture<NettyRpcChannel>> channelPool;

  ProtoRpcRaftClient(@Nonnull ObjectPool<CompletableFuture<NettyRpcChannel>> channelPool) {
    this.channelPool = checkNotNull(channelPool);
  }

  @Nonnull
  public CompletableFuture<RequestVoteResponse> requestVote(@Nonnull RequestVote request) {
    checkNotNull(request);
    return call(RpcCall.requestVote(ProtoUtils.convert(request)))
        .thenApply(ProtoUtils.convertVoteResponse);
  }

  @Nonnull
  public CompletableFuture<AppendEntriesResponse> appendEntries(@Nonnull AppendEntries request) {
    checkNotNull(request);
    return call(RpcCall.appendEntries(ProtoUtils.convert(request)))
        .thenApply(ProtoUtils.convertAppendResponse);
  }

  private <T> CompletableFuture<T> call(final RpcCall<T> call) {

    CompletableFuture<NettyRpcChannel> channel = null;
    try {

      channel = channelPool.borrowObject();
      CompletableFuture<T> response = channel.thenCompose(channel1 -> {
        RaftProto.RaftService.Stub stub = RaftProto.RaftService.newStub(channel1);
        ClientController controller = new ClientController(channel1);
        controller.setTimeout(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        CompletableFuture<T> future = new CompletableFuture<>();
        call.call(stub, controller, (result) -> {
          if (future.isCancelled()) {
            return;
          }

          if (null == result) {
            future.completeExceptionally(new RaftException(controller.errorText()));
          } else {
            future.complete(result);
          }
        });
        return future;
      });

      CompletableFuture<NettyRpcChannel> finalChannel = channel;
      response.thenAccept((ignore) -> {
        try {
          channelPool.returnObject(finalChannel);
        } catch (Exception ignored) {
        }
      });

      return response;

    } catch (Exception e) {

      try {
        channelPool.invalidateObject(channel);
      } catch (Exception ignored) {
      }
      channel = null;
      CompletableFuture<T> failed = new CompletableFuture<>();
      failed.completeExceptionally(e);
      return failed;

    } finally {

      try {
        if (null != channel) {
          channelPool.returnObject(channel);
        }
      } catch (Exception ignored) {
      }

    }
  }

  private static abstract class RpcCall<T> {

    abstract void call(RaftProto.RaftService.Stub stub, RpcController controller, RpcCallback<T> callback);

    static RpcCall<RaftProto.AppendEntriesResponse> appendEntries(final RaftProto.AppendEntries request) {
      return new RpcCall<RaftProto.AppendEntriesResponse>() {
        @Override
        public void call(RaftProto.RaftService.Stub stub, RpcController controller, RpcCallback<RaftProto.AppendEntriesResponse> callback) {
          stub.appendEntries(controller, request, callback);
        }
      };
    }

    static RpcCall<RaftProto.RequestVoteResponse> requestVote(final RaftProto.RequestVote request) {
      return new RpcCall<RaftProto.RequestVoteResponse>() {
        @Override
        public void call(RaftProto.RaftService.Stub stub, RpcController controller, RpcCallback<RaftProto.RequestVoteResponse> callback) {
          stub.requestVote(controller, request, callback);
        }
      };
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

}
