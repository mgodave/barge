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
import org.robotninjas.barge.annotations.RaftExecutor;
import org.robotninjas.barge.context.RaftContext;
import org.robotninjas.barge.rpc.ClientProto;
import org.robotninjas.barge.rpc.RaftProto;
import org.robotninjas.barge.state.Context;
import org.robotninjas.protobuf.netty.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.robotninjas.barge.rpc.ClientProto.CommitOperation;
import static org.robotninjas.barge.rpc.ClientProto.CommitOperationResponse;
import static org.robotninjas.barge.rpc.RaftProto.*;

public class DefaultRaftService extends AbstractService
  implements RaftProto.RaftService.Interface, ClientProto.ClientService.Interface, RaftService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftService.class);

  private final RpcServer rpcServer;
  private final Context ctx;
  private final RaftContext rctx;
  private final ListeningExecutorService executor;

  @Inject
  DefaultRaftService(RpcServer rpcServer, Context ctx, RaftContext rctx, @RaftExecutor ListeningExecutorService executor) {
    this.rpcServer = rpcServer;
    this.ctx = ctx;
    this.rctx = rctx;
    this.executor = executor;
  }

  @Override
  protected void doStart() {

    try {

      // Due to the fact that MDC uses ThreadLocal state to store the properties map
      // we need to make sure these properties are initialized on the Raft Thread
      executor.submit(new Runnable() {
        @Override
        public void run() {
          MDC.put("state", ctx.getState().toString());
          MDC.put("term", Long.toString(rctx.term()));
        }
      }).get();

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
  public synchronized void requestVote(RpcController controller, RequestVote request, RpcCallback<RequestVoteResponse> done) {
    try {
      done.run(ctx.requestVote(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing RequestVote", e);
      controller.setFailed(e.getMessage());
      done.run(null);
    }
  }

  @Override
  public synchronized void appendEntries(RpcController controller, AppendEntries request, RpcCallback<AppendEntriesResponse> done) {
    try {
      done.run(ctx.appendEntries(request));
    } catch (Exception e) {
      LOGGER.debug("Exception caught servicing AppendEntries", e);
      controller.setFailed(e.getMessage());
      done.run(null);
    }
  }

  public synchronized ListenableFuture<CommitOperationResponse> commitOperationAsync(CommitOperation request) {
    // Run the operation on the raft thread
    ListenableFuture<ListenableFuture<CommitOperationResponse>> response =
      executor.submit(new Callable<ListenableFuture<CommitOperationResponse>>() {
        @Override
        public ListenableFuture<CommitOperationResponse> call() throws Exception {
          return Futures.immediateFailedFuture(new Exception("Not Implemented"));
        }
      });
    return Futures.dereference(response);
  }

  @Override
  public synchronized CommitOperationResponse commitOperation(CommitOperation request) throws ExecutionException, InterruptedException {
    return commitOperationAsync(request).get();
  }

  @Override
  public synchronized void commitOperation(final RpcController controller, CommitOperation request, final RpcCallback<CommitOperationResponse> done) {

    ListenableFuture<CommitOperationResponse> response = commitOperationAsync(request);
    Futures.addCallback(response, new FutureCallback<CommitOperationResponse>() {

      @Override
      public void onSuccess(@Nullable CommitOperationResponse result) {
        done.run(result);
      }

      @Override
      public void onFailure(Throwable t) {
        controller.setFailed(t.getMessage());
        done.run(null);
      }

    });

  }

}
