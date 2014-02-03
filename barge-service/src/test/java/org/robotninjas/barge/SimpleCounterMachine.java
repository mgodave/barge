package org.robotninjas.barge;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class SimpleCounterMachine implements StateMachine {

  private final int id;
  private final List<Replica> replicas;

  private int counter;
  private File logDirectory;
  private NettyRaftService service;

  public SimpleCounterMachine(int id, List<Replica> replicas) {
    checkArgument(id >= 0 && id < replicas.size(), "replica id " + id + " should be between 0 and " + replicas.size());

    this.id = id;
    this.replicas = replicas;
  }

  @SuppressWarnings({"ConstantConditions", "ResultOfMethodCallIgnored"})
  private static void delete(File directory) {
    for (File file : directory.listFiles()) {
      if (file.isFile()) {
        file.delete();
      } else {
        delete(file);
      }
    }
    directory.delete();
  }

  @Override
  public Object applyOperation(@Nonnull ByteBuffer entry) {
    return this.counter++;
  }

  public void startRaft() {
    int clusterSize = replicas.size();
    Replica[] configuration = new Replica[clusterSize - 1];
    for (int i = 0; i < clusterSize - 1; i++) {
      configuration[i] = replicas.get((id + i + 1) % clusterSize);
    }

    ClusterConfig config1 = ClusterConfig.from(replicas.get(id % clusterSize), configuration);

    NettyRaftService service1 = NettyRaftService.newBuilder(config1)
      .logDir(logDirectory)
      .timeout(500)
      .build(this);

    service1.startAsync().awaitRunning();
    this.service = service1;
  }

  public File makeLogDirectory(File parentDirectory) throws IOException {
    this.logDirectory = new File(parentDirectory, "log" + id);

    if (logDirectory.exists()) {
      delete(logDirectory);
    }

    if (!logDirectory.exists() && !logDirectory.mkdirs()) {
      throw new IllegalStateException("cannot create log directory " + logDirectory);
    }

    return logDirectory;
  }


  public void commit(byte[] bytes) throws InterruptedException, RaftException {
    service.commit(bytes);
  }

  public void stop() {
    service.stopAsync().awaitTerminated();
  }

  public void deleteLogDirectory() {
    delete(logDirectory);
  }

  /**
   * Wait at most {@code timeout} for this counter to reach value {@code increments}.
   *
   * @param increments value expected for counter.
   * @param timeout    timeout in ms.
   * @throws IllegalStateException if {@code expected} is not reached at end of timeout.
   */
  public void waitForValue(int increments, long timeout) {
    long start = System.nanoTime();
    while (counter != increments && (elapsedMs(start) < timeout)) {
      Thread.yield();
    }

    if (counter != increments) {
      throw new IllegalStateException("counter' value " + counter + " is not equal to expected " + increments + " within " + timeout + "ms");
    }
  }

  private long elapsedMs(long start) {
    return (System.nanoTime() - start) / 1000000;
  }
}
