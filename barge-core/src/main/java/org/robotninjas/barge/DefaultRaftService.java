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

import com.google.common.util.concurrent.*;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.robotninjas.barge.proto.ClientProto;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.state.Context;
import org.robotninjas.protobuf.netty.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static org.robotninjas.barge.proto.ClientProto.CommitOperation;
import static org.robotninjas.barge.proto.ClientProto.CommitOperationResponse;
import static org.robotninjas.barge.proto.RaftProto.*;

@ThreadSafe
@Immutable
class DefaultRaftService extends AbstractService
  implements RaftProto.RaftService.Interface, ClientProto.ClientService.Interface, RaftService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftService.class);

  private final RpcServer rpcServer;
  private final Context ctx;
  private final ListeningExecutorService executor;

  @Inject
  DefaultRaftService(@RaftExecutor @Nonnull ListeningExecutorService executor,
                     @Nonnull RpcServer rpcServer,
                     @Nonnull Context ctx) {

    this.rpcServer = checkNotNull(rpcServer);
    this.ctx = checkNotNull(ctx);
    this.executor = checkNotNull(executor);

  }

  @Override
  protected void doStart() {

    try {

      // Due to the fact that MDC uses ThreadLocal state to store the properties map
      // we need to make sure these properties are initialized on the Raft Thread
      executor.submit(new Runnable() {
        @Override
        public void run() {
          MDC.put("state", "START");
          MDC.put("term", "0");
        }
      }).get(10, TimeUnit.SECONDS);

      Service replicaService = RaftProto.RaftService.newReflectiveService(this);
      rpcServer.registerService(replicaService);

      Service clientService = ClientProto.ClientService.newReflectiveService(this);
      rpcServer.registerService(clientService);

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
  public synchronized void requestVote(@Nonnull RpcController controller, @Nonnull RequestVote request, @Nonnull RpcCallback<RequestVoteResponse> done) {
    try {
      done.run(ctx.requestVote(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing RequestVote", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  @Override
  public synchronized void appendEntries(@Nonnull RpcController controller, @Nonnull AppendEntries request, @Nonnull RpcCallback<AppendEntriesResponse> done) {
    try {
      done.run(ctx.appendEntries(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing AppendEntries", e);
      controller.setFailed(nullToEmpty(e.getMessage()));
      done.run(null);
    }
  }

  public synchronized ListenableFuture<CommitOperationResponse> commitOperationAsync(@Nonnull final CommitOperation request) {
    // Run the operation on the raft thread
    ListenableFuture<ListenableFuture<CommitOperationResponse>> response =
      executor.submit(new Callable<ListenableFuture<CommitOperationResponse>>() {
        @Override
        public ListenableFuture<CommitOperationResponse> call() throws Exception {
          return ctx.commitOperation(request);
        }
      });
    return Futures.dereference(response);
  }

  @Override
  public void installSnapshot(RpcController controller, InstallSnapshot request, RpcCallback<InstallSnapshotResponse> done) {

  }

  @Override
  public synchronized void commitOperation(@Nonnull final RpcController controller, @Nonnull CommitOperation request, @Nonnull final RpcCallback<CommitOperationResponse> done) {

    ListenableFuture<CommitOperationResponse> response = commitOperationAsync(request);
    Futures.addCallback(response, new FutureCallback<CommitOperationResponse>() {

      @Override
      public void onSuccess(@Nullable CommitOperationResponse result) {
        done.run(result);
      }

      @Override
      public void onFailure(@Nonnull Throwable t) {
        LOGGER.debug("Exception caught servicing CommitOperation", t);
        controller.setFailed(nullToEmpty(t.getMessage()));
        done.run(null);
      }

    });

  }

}
