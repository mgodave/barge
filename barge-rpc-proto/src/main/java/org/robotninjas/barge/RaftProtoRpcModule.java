package org.robotninjas.barge;

import com.google.inject.PrivateModule;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.Optional;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.barge.rpc.RpcModule;
import org.robotninjas.protobuf.netty.server.RpcServer;

class RaftProtoRpcModule extends PrivateModule {

  private final NettyReplica localEndpoint;
  private Optional<NioEventLoopGroup> eventLoopGroup = Optional.empty();

  public RaftProtoRpcModule(NettyReplica localEndpoint) {
    this.localEndpoint = localEndpoint;
  }

  @Override
  protected void configure() {
    final NioEventLoopGroup eventLoop  = initializeEventLoop();

    install(new RpcModule(localEndpoint.address(), eventLoop));

    expose(RpcServer.class);
    expose(RaftClientProvider.class);
  }

  private NioEventLoopGroup initializeEventLoop() {
    final NioEventLoopGroup eventLoop;

    if (eventLoopGroup.isPresent()) {
      eventLoop = eventLoopGroup.get();
    } else {
      eventLoop = new NioEventLoopGroup(1);
      Runtime.getRuntime().addShutdownHook(
          new Thread(eventLoop::shutdownGracefully)
      );
    }
    return eventLoop;
  }

}
