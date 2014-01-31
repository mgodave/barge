package org.robotninjas.barge;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyRaftServiceTest {

  private static final File target = new File(System.getProperty("basedir", "."), "target");

  private final Replica replicas[] = new Replica[]{
      Replica.fromString("localhost:10001"),
      Replica.fromString("localhost:10002"),
      Replica.fromString("localhost:10003")};

  private final SimpleCounterMachine[] counters = new SimpleCounterMachine[]{new SimpleCounterMachine(0, replicas), new SimpleCounterMachine(1, replicas), new SimpleCounterMachine(2, replicas)};

  @Before
  public void setup() throws IOException {
    for (int i = 0; i < 3; i++) {
      counters[i].makeLogDirectory(target);
      counters[i].startRaft();
    }
  }

  @Ignore("test failing with inconsistent values observed between replicas")
  @Test(timeout = 30000)
  public void canRun3RaftInstancesReachingCommonState() throws Exception {
    Thread.sleep(1000);

    int incrementBy = 10;

    for (int i = 0; i < incrementBy; i++) {
      counters[0].commit(new byte[]{1});
    }

    assertThat(new int[]{
        counters[0].getCounter(),
        counters[1].getCounter(),
        counters[2].getCounter()}).isEqualTo(new int[]{10, 10, 10});
  }

  @After
  public void tearDown() throws Exception {
    for (int i = 0; i < 3; i++) {
      counters[i].stop();
    }
  }


}
