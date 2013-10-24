package org.robotninjas.barge.test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.RaftService;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;

public class Test implements StateMachine {

  private final String fileName;
  private PrintStream out;

  public Test(String fileName) {
    this.fileName = fileName;
  }

  public void init() throws IOException {
    this.out = new PrintStream(new FileOutputStream(fileName));
  }

  @Override
  public void applyOperation(@Nonnull ByteBuffer entry) {
    out.println(entry.getLong());
  }

  public void shutdown() {
    this.out.close();
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

    Test machine = new Test(args[0] + "/out");
    machine.init();

    RaftService raft = RaftService.newBuilder()
      .local(local)
      .members(members)
      .logDir(logDir)
      .timeout(300)
      .build(machine);

    raft.startAsync().awaitRunning();

    Thread.sleep(20000);

    RateLimiter limiter = RateLimiter.create(10000);
    try {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      for (long i = 0; i < 100000; ++i) {
        System.out.println("Sending " + i);
        limiter.acquire();
        buffer.putLong(i).rewind();
        raft.commit(buffer.array());
      }
      System.out.println("done.");
    } catch (RaftException e) {
      System.out.println("Not leader");
      //e.printStackTrace();
      //NOT LEADER, ignore
    }

  }

}
