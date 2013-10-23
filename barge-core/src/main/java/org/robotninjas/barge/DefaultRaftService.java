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

package org.robotninjas.barge;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.state.Context;
import org.robotninjas.protobuf.netty.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static org.robotninjas.barge.proto.RaftProto.*;

@ThreadSafe
@Immutable
class DefaultRaftService extends AbstractService
  implements RaftProto.RaftService.Interface, RaftService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftService.class);

  private final ListeningExecutorService executor;
  private final RpcServer rpcServer;
  private final Context ctx;

  @Inject
  DefaultRaftService(@Nonnull RpcServer rpcServer, @RaftExecutor ListeningExecutorService executor, @Nonnull Context ctx) {

    this.executor = checkNotNull(executor);
    this.rpcServer = checkNotNull(rpcServer);
    this.ctx = checkNotNull(ctx);

  }

  @Override
  protected void doStart() {

    try {

      Service replicaService = RaftProto.RaftService.newReflectiveService(this);
      rpcServer.registerService(replicaService);

      rpcServer.startAsync().awaitRunning();

      notifyStarted();

    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  @Override
  protected void doStop() {

    try {
      rpcServer.stopAsync().awaitTerminated();
      notifyStarted();
    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  @Override
  public void requestVote(@Nonnull RpcController controller, @Nonnull RequestVote request, @Nonnull RpcCallback<RequestVoteResponse> done) {
    try {
      done.run(ctx.requestVote(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing RequestVote", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  @Override
  public void appendEntries(@Nonnull RpcController controller, @Nonnull AppendEntries request, @Nonnull RpcCallback<AppendEntriesResponse> done) {
    try {
      done.run(ctx.appendEntries(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing AppendEntries", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  public ListenableFuture<Boolean> commit(final byte[] operation) throws RaftException {

    // Make sure this happens on the Raft thread
    ListenableFuture<ListenableFuture<Boolean>> response =
      executor.submit(new Callable<ListenableFuture<Boolean>>() {
        @Override
        public ListenableFuture<Boolean> call() throws Exception {
          return ctx.commitOperation(operation);
        }
      });

    return Futures.dereference(response);

  }


}
