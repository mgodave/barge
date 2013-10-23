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
import com.google.inject.Injector;
import com.google.protobuf.Service;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.state.Context;
import org.robotninjas.protobuf.netty.server.RpcServer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
@Immutable
public class RaftService extends AbstractService {

  private final ListeningExecutorService executor;
  private final RpcServer rpcServer;
  private final Context ctx;

  @Inject
  RaftService(@Nonnull RpcServer rpcServer, @RaftExecutor ListeningExecutorService executor, @Nonnull Context ctx) {

    this.executor = checkNotNull(executor);
    this.rpcServer = checkNotNull(rpcServer);
    this.ctx = checkNotNull(ctx);

  }

  @Override
  protected void doStart() {

    try {

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
      notifyStarted();
    } catch (Exception e) {
      notifyFailed(e);
    }

  }

  public ListenableFuture<Boolean> commit(final byte[] operation) throws RaftException {

    // Make sure this happens on the Barge thread
    ListenableFuture<ListenableFuture<Boolean>> response =
      executor.submit(new Callable<ListenableFuture<Boolean>>() {
        @Override
        public ListenableFuture<Boolean> call() throws Exception {
          return ctx.commitOperation(operation);
        }
      });

    return Futures.dereference(response);

  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private static long TIMEOUT = 150;

    private File logDir = Files.createTempDir();
    private Replica local = Replica.fromString("localhost:10000");
    private long timeout = TIMEOUT;
    private List<Replica> members = Collections.emptyList();

    protected Builder() {}

    public Builder timeout(long to, TimeUnit unit) {
      this.timeout = unit.convert(to, TimeUnit.MILLISECONDS);
      return this;
    }

    public Builder logDir(File logDir) {
      this.logDir = logDir;
      return this;
    }

    public Builder local(Replica local) {
      this.local = local;
      return this;
    }

    public Builder members(List<Replica> members) {
      this.members = members;
      return this;
    }

    public RaftService build(StateMachine stateMachine) {
      RaftModule raftModule = new RaftModule(local, members, timeout, logDir, stateMachine);
      Injector injector = Guice.createInjector(raftModule);
      RaftServiceFactory factory = injector.getInstance(RaftServiceFactory.class);
      RaftService service = factory.create(stateMachine);
      return service;
    }

  }


}
