package org.robotninjas.barge;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.PrivateModule;
import io.netty.channel.nio.NioEventLoopGroup;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.barge.rpc.RaftExecutor;
import org.robotninjas.barge.rpc.RaftScheduler;
import org.robotninjas.barge.rpc.RpcModule;
import org.robotninjas.protobuf.netty.server.RpcServer;

import java.util.concurrent.ScheduledExecutorService;

class RaftProtoRpcModule extends PrivateModule {

  private final Replica localEndpoint;
  private Optional<NioEventLoopGroup> eventLoopGroup = Optional.absent();

  public RaftProtoRpcModule(Replica localEndpoint) {
    this.localEndpoint = localEndpoint;
  }

  @Override
  protected void configure() {
    final NioEventLoopGroup eventLoop  = initializeEventLoop();

    install(new RpcModule(localEndpoint.address(), eventLoop));

    expose(ListeningExecutorService.class)
        .annotatedWith(RaftExecutor.class);
    expose(ScheduledExecutorService.class)
        .annotatedWith(RaftScheduler.class);
    expose(RpcServer.class);
    expose(RaftClientProvider.class);
  }

  private NioEventLoopGroup initializeEventLoop() {
    final NioEventLoopGroup eventLoop;

    if (eventLoopGroup.isPresent()) {
      eventLoop = eventLoopGroup.get();
    } else {
      eventLoop = new NioEventLoopGroup(1);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          eventLoop.shutdownGracefully();
        }
      });
    }
    return eventLoop;
  }

}
