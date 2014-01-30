package org.robotninjas.barge;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyServiceTest {

  private static final File target = new File(System.getProperty("basedir", "."), "target");

  private final Replica replicas[] = new Replica[]{
      Replica.fromString("localhost:10001"),
      Replica.fromString("localhost:10002"),
      Replica.fromString("localhost:10003")};

  private final Counter[] counters = new Counter[]{new Counter(), new Counter(), new Counter()};
  private final RaftService[] services = new RaftService[3];
  private final File[] logDirectories = new File[3];

  @Before
  public void setup() throws IOException {
    for (int i = 0; i < 3; i++) {
      logDirectories[i] = makeLogDirectory(i);
      services[i] = startRaft(i, logDirectories[i]);
    }
  }

  @Ignore("test currently failing, goes into a loop in commit without making any progress")
  @Test(timeout = 30000)
  public void canRun3RaftInstancesReachingCommonState() throws Exception {
    Thread.sleep(1000);

    int incrementBy = 10;

    for (int i = 0; i < incrementBy; i++) {
      services[0].commit(new byte[]{1});
    }

    assertThat(counters[0].getCounter()).isEqualTo(incrementBy);
    assertThat(counters[1].getCounter()).isEqualTo(incrementBy);
    assertThat(counters[2].getCounter()).isEqualTo(incrementBy);
  }

  @After
  public void tearDown() throws Exception {
    for (int i = 0; i < 3; i++) {
      services[i].stopAsync().awaitTerminated();
      deleteLogDirectory(logDirectories[i]);
    }
  }

  @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
  private void deleteLogDirectory(File directory) throws IOException {
    for (File file : directory.listFiles()) {
      if (file.isFile())
        file.delete();
      else
        deleteLogDirectory(file);
    }
    directory.delete();
  }

  private RaftService startRaft(int replicaId, File logDirectory) {
    ClusterConfig config1 = ClusterConfig.from(replicas[replicaId % 3],
        replicas[(replicaId + 1) % 3],
        replicas[(replicaId + 2) % 3]);

    RaftService service1 = RaftService.newBuilder(config1)
        .logDir(logDirectory)
        .timeout(500)
        .build(counters[replicaId]);

    service1.startAsync().awaitRunning();
    return service1;
  }

  private File makeLogDirectory(int replicaId) throws IOException {
    File logDir1 = new File(target, "log" + replicaId);

    if (logDir1.exists()) {
      deleteLogDirectory(logDir1);
    }

    if (!logDir1.exists() && !logDir1.mkdirs()) {
      throw new IllegalStateException("cannot create log directory " + logDir1);
    }
    return logDir1;
  }

  public static class Counter implements StateMachine {

    private int counter;

    @Override
    public Object applyOperation(@Nonnull ByteBuffer entry) {
      this.counter++;
      return 0;
    }

    public int getCounter() {
      return counter;
    }
  }
}
