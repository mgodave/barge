package org.robotninjas.barge;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.robotninjas.protobuf.netty.server.RpcServer;
import org.robotninjas.barge.rpc.ClientProto;
import org.robotninjas.barge.rpc.RaftProto;
import org.robotninjas.barge.state.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.concurrent.ExecutionException;

import static org.robotninjas.barge.rpc.ClientProto.CommitOperation;
import static org.robotninjas.barge.rpc.ClientProto.CommitOperationResponse;
import static org.robotninjas.barge.rpc.RaftProto.*;

public class DefaultRaftService extends AbstractService
  implements RaftProto.RaftService.Interface, ClientProto.ClientService.Interface, RaftService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftService.class);

  private final RpcServer rpcServer;
  private final Context ctx;

  @Inject
  DefaultRaftService(RpcServer rpcServer, Context ctx) {
    this.rpcServer = rpcServer;
    this.ctx = ctx;
  }

  @Override
  protected void doStart() {

    try {
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
    return Futures.immediateFailedFuture(new Exception("Not implemented"));
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
