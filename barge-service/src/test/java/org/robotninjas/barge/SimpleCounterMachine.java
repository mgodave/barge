package org.robotninjas.barge;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SimpleCounterMachine implements StateMachine {

  private final int id;
  private final Replica[] replicas;

  private int counter;
  private File logDirectory;
  private NettyRaftService service;

  public SimpleCounterMachine(int id, Replica[] replicas) {
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
    this.counter++;
    System.err.println("incrementing counter of " + id + " to " + counter);
    return 0;
  }

  public int getCounter() {
    return counter;
  }

  public void startRaft() {
    ClusterConfig config1 = ClusterConfig.from(replicas[id % 3],
        replicas[(id + 1) % 3],
        replicas[(id + 2) % 3]);

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
    delete(logDirectory);
  }
}
