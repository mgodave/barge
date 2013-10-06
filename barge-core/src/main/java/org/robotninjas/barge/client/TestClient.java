package org.robotninjas.barge.client;

import com.github.rholder.retry.RetryException;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.robotninjas.barge.RaftException;
import org.robotninjas.protobuf.netty.client.RpcClient;

import java.nio.ByteBuffer;

public class TestClient {

  public static void main(String... args) throws InterruptedException {

    NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
    EventExecutorGroup eventExecutorGroup = new DefaultEventExecutorGroup(1);
    RpcClient rpcClient = new RpcClient(eventLoopGroup, eventExecutorGroup);

    BargeClient client = new BargeClient(rpcClient, "client1", 0, "localhost:10000");

    ByteBuffer payload = ByteBuffer.allocate(8);

    for (long value = 0; ; ++value) {

      try {

        payload.putLong(value).rewind().rewind();

        boolean committed = client.commit(payload.array());
        System.out.println(committed);

      } catch (RetryException e) {
        System.out.println("Max retries");
        value--;
      } catch (RaftException e) {
        System.out.println(e.getMessage());
      }

//      Thread.sleep(10);

    }

  }

}
