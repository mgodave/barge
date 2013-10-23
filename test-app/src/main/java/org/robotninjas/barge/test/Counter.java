package org.robotninjas.barge.test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.robotninjas.barge.Raft;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.protobuf.netty.client.RpcClient;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.robotninjas.barge.test.proto.CounterProto.CounterOp;

public class Counter {

  public static void main(String... args) throws ExecutionException, InterruptedException {

    List<Replica> members = Lists.newArrayList(
      Replica.fromString("localhost:10000"),
      Replica.fromString("localhost:10001"),
      Replica.fromString("localhost:10002")
    );

    for (Replica local : members) {

      List<Replica> m = Lists.newArrayList(members);
      m.remove(local);

      File logDir = Files.createTempDir();

      RaftService service = Raft.newDistributedStateMachine(local, m, logDir, new StateMachine() {

        long count = 0;

        @Override
        public void applyOperation(@Nonnull ByteBuffer entry) {
          try {
            CounterOp op = CounterOp.parseFrom(ByteString.copyFrom(entry));
            count += op.getAmount();
            System.out.println("count " + count);
          } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
          }
        }
      });

      service.startAsync().awaitRunning();

    }

    NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    EventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(1);
    RpcClient rpcClient = new RpcClient(eventLoopGroup, eventExecutorGroup);

    ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
    List<ListenableFuture<?>> clients = Lists.newArrayListWithCapacity(3);
    for (int i = 0; i < 3; i++) {
      CounterClient client = new CounterClient(rpcClient, "client" + i);
      clients.add(executor.submit(client));
    }

    Futures.allAsList(clients).get();

  }

  public static class CounterClient implements Runnable {


   // private final BargeClient client;

    public CounterClient(RpcClient rpcClient, String id) {
      //this.client = new BargeClient(rpcClient, id, 0, "localhost:10000");
    }

    @Override
    public void run() {

      CounterOp op = CounterOp.newBuilder().setAmount(1).build();

      for (int i = 0; i < 1000; ++i) {

//        try {
//          client.commit(op.toByteArray());
//        } catch (RaftException e) {
//          e.printStackTrace();
//        } catch (RetryException e) {
//          --i;
//        }

      }

    }

  }

}
