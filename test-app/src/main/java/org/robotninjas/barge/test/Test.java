package org.robotninjas.barge.test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

public class Test implements StateMachine {

  @Override
  public void applyOperation(@Nonnull ByteBuffer entry) {
    System.out.println(entry.getLong());
  }

  public static void main(String... args) throws Exception {

    final int port = Integer.parseInt(args[0]);

    Replica local = Replica.fromString("localhost:" + port);
    List<Replica> members = Lists.newArrayList(
      Replica.fromString("localhost:10000"),
      Replica.fromString("localhost:10001"),
      Replica.fromString("localhost:10002")
    );
    members.remove(local);

    File logDir = new File(args[0]);
    logDir.mkdir();

    StateMachine machine = new Test();

    RaftService raft = RaftService.newBuilder()
      .local(local)
      .members(members)
      .logDir(logDir)
      .timeout(150)
      .build(machine);

    raft.startAsync().awaitRunning();

//    while (true) {

      Thread.sleep(10000);

      RateLimiter limiter = RateLimiter.create(200);
      try {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        for (long i = 0; i < 100000; ++i) {
          limiter.acquire();
          buffer.putLong(i).rewind();
          raft.commit(buffer.array());
        }
      } catch (RaftException e) {
        //e.printStackTrace();
        //NOT LEADER, ignore
      }

//    }
  }

}
