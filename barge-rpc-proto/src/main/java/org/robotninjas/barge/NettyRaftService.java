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

import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Guice;
import com.google.protobuf.Service;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.service.RaftService;
import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.state.StateTransitionListener;
import org.robotninjas.protobuf.netty.server.RpcServer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;

import static org.robotninjas.barge.state.Raft.StateType.*;

@ThreadSafe
@Immutable
public class NettyRaftService extends AbstractService implements RaftService {

  private final ListeningExecutorService executor;
  private final RpcServer rpcServer;
  private final Raft ctx;

  @Inject
  NettyRaftService(@Nonnull RpcServer rpcServer, @RaftExecutor ListeningExecutorService executor, @Nonnull Raft ctx) {

    this.executor = checkNotNull(executor);
    this.rpcServer = checkNotNull(rpcServer);
    this.ctx = checkNotNull(ctx);

  }

  @Override
  protected void doStart() {

    try {

      ctx.setState(null, START);

      configureRpcServer();
      rpcServer.startAsync().awaitRunning();

      notifyStarted();

    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  private void configureRpcServer() {
    RaftServiceEndpoint endpoint = new RaftServiceEndpoint(ctx);
    Service replicaService = RaftProto.RaftService.newReflectiveService(endpoint);
    rpcServer.registerService(replicaService);
  }

  @Override
  protected void doStop() {

    try {
      rpcServer.stopAsync().awaitTerminated();
      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  @Override
  public ListenableFuture<Object> commitAsync(final byte[] operation) throws RaftException {

    // Make sure this happens on the Barge thread
    ListenableFuture<ListenableFuture<Object>> response =
      executor.submit(new Callable<ListenableFuture<Object>>() {
        @Override
        public ListenableFuture<Object> call() throws Exception {
          return ctx.commitOperation(operation);
        }
      });

    return Futures.dereference(response);

  }

  @Override
  public Object commit(final byte[] operation) throws RaftException, InterruptedException {
    try {
      return commitAsync(operation).get();
    } catch (ExecutionException e) {
      propagateIfInstanceOf(e.getCause(), NotLeaderException.class);
      propagateIfInstanceOf(e.getCause(), NoLeaderException.class);
      throw propagate(e.getCause());
    }
  }

  public static Builder newBuilder(ClusterConfig config) {
    return new Builder(config);
  }

  private void addTransitionListener(StateTransitionListener listener) {
    ctx.addTransitionListener(listener);
  }

  public static class Builder {

    private static long TIMEOUT = 150;

    private final ClusterConfig config;
    private File logDir = Files.createTempDir();
    private long timeout = TIMEOUT;
    private StateTransitionListener listener;

    protected Builder(ClusterConfig config) {
      this.config = config;
    }

    public Builder timeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder logDir(File logDir) {
      this.logDir = logDir;
      return this;
    }

    public NettyRaftService build(StateMachine stateMachine) {
      NettyRaftService nettyRaftService = Guice.createInjector(
        new NettyRaftModule(config, logDir, stateMachine, timeout))
        .getInstance(NettyRaftService.class);

      nettyRaftService.addTransitionListener(listener);

      return nettyRaftService;
    }

    public Builder transitionListener(StateTransitionListener listener) {
      this.listener = listener;
      return this;
    }
  }

}
