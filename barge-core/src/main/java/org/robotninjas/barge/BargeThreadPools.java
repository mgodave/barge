package org.robotninjas.barge;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public class BargeThreadPools implements Closeable {

  private static final int NUM_THREADS = 1;

  boolean closed;
  final NioEventLoopGroup raftEventLoop;
  final boolean closeEventLoop;
  final ListeningExecutorService stateMachineExecutor;
  final boolean closeStateMachineExecutor;
  final ListeningScheduledExecutorService raftExecutor;

  public BargeThreadPools(Optional<NioEventLoopGroup> eventLoopGroup,
      Optional<ListeningExecutorService> stateMachineExecutor) {
    if (eventLoopGroup.isPresent()) {
      this.raftEventLoop = eventLoopGroup.get();
      this.closeEventLoop = false;
    } else {
      this.raftEventLoop = new NioEventLoopGroup(1, new DefaultThreadFactory("pool-raft"));
      this.closeEventLoop = true;
      // Runtime.getRuntime().addShutdownHook(new Thread() {
      // public void run() {
      // eventLoop.shutdownGracefully();
      // }
      // });
    }

    this.raftExecutor = MoreExecutors.listeningDecorator(this.raftEventLoop);
    
    if (stateMachineExecutor.isPresent()) {
      this.stateMachineExecutor = stateMachineExecutor.get();
      this.closeStateMachineExecutor = false;
    } else {
      this.stateMachineExecutor = MoreExecutors.listeningDecorator(
          Executors.newSingleThreadExecutor(new DefaultThreadFactory("pool-raft-worker")));
      this.closeStateMachineExecutor = true;
      //
      // Runtime.getRuntime().addShutdownHook(new Thread() {
      // public void run() {
      // executor.shutdownNow();
      // }
      // });
    }

  }

  public NioEventLoopGroup getEventLoopGroup() {
    Preconditions.checkState(!closed);
    return raftEventLoop;
  }

  @Override
  public void close() throws IOException {
    if (closeEventLoop) {
      raftEventLoop.shutdownGracefully();
    }
    if (closeStateMachineExecutor) {
      stateMachineExecutor.shutdown();
    }
    closed = true;
  }

  public ListeningExecutorService getStateMachineExecutor() {
    Preconditions.checkState(!closed);
    return stateMachineExecutor;
  }

  public ListeningExecutorService getRaftExecutor() {
    return raftExecutor;
  }

  public ScheduledExecutorService getRaftScheduler() {
    return raftExecutor;
  }

}
