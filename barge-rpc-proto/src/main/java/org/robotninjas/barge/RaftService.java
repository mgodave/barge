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

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.google.common.util.concurrent.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.Service;
import io.netty.channel.nio.NioEventLoopGroup;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.state.RaftStateContext;
import org.robotninjas.protobuf.netty.server.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static org.robotninjas.barge.state.RaftStateContext.StateType.START;

@ThreadSafe
@Immutable
public class RaftService extends AbstractService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftService.class);

  private final ListeningExecutorService executor;
  private final RpcServer rpcServer;
  private final RaftStateContext ctx;

  @Inject
  RaftService(@Nonnull RpcServer rpcServer, @RaftExecutor ListeningExecutorService executor, @Nonnull RaftStateContext ctx) {

    this.executor = checkNotNull(executor);
    this.rpcServer = checkNotNull(rpcServer);
    this.ctx = checkNotNull(ctx);

  }

  @Override
  protected void doStart() {

    try {

      ctx.setState(null, START);

      RaftServiceEndpoint endpoint = new RaftServiceEndpoint(ctx);
      Service replicaService = RaftProto.RaftService.newReflectiveService(endpoint);
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
      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }

  }

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

  public static class Builder {

    private static long TIMEOUT = 150;

    private final ClusterConfig config;
    private File logDir = Files.createTempDir();
    private long timeout = TIMEOUT;
    private Optional<NioEventLoopGroup> eventLoop = Optional.absent();
    private Optional<ListeningExecutorService> stateExecutor = Optional.absent();

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

    public Builder eventLoop(NioEventLoopGroup eventLoop) {
      this.eventLoop = Optional.of(eventLoop);
      return this;
    }

    public Builder stateExecutor(ExecutorService executor) {
      this.stateExecutor = Optional.of(MoreExecutors.listeningDecorator(executor));
      return this;
    }

    public RaftService build(StateMachine stateMachine) {

        RaftProtoRpcModule raftProtoRpcModule = new RaftProtoRpcModule(config, logDir, stateMachine);
      raftProtoRpcModule.setTimeout(timeout);
      if (eventLoop.isPresent()) {
        raftProtoRpcModule.setNioEventLoop(eventLoop.get());
      }
      if (stateExecutor.isPresent()) {
        raftProtoRpcModule.setStateMachineExecutor(stateExecutor.get());
      }

      Injector injector = Guice.createInjector(raftProtoRpcModule);
      return injector.getInstance(RaftService.class);
    }

  }


}
