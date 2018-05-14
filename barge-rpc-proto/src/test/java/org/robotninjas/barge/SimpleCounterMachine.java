package org.robotninjas.barge;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nonnull;
import org.robotninjas.barge.utils.Files;
import org.robotninjas.barge.utils.Prober;


public class SimpleCounterMachine implements StateMachine {

  private final int id;
  private final List<NettyReplica> replicas;
  private final GroupOfCounters groupOfCounters;

  private int counter;
  private File logDirectory;
  private NettyRaftService service;

  public SimpleCounterMachine(int id, List<NettyReplica> replicas, GroupOfCounters groupOfCounters) {
    checkArgument((id >= 0) && (id < replicas.size()), "replica id " + id + " should be between 0 and " + replicas.size());

    this.groupOfCounters = groupOfCounters;
    this.id = id;
    this.replicas = replicas;
  }

  @Override
  public Object applyOperation(@Nonnull ByteBuffer entry) {
    return this.counter++;
  }

  public void startRaft() {
    int clusterSize = replicas.size();
    NettyReplica[] configuration = new NettyReplica[clusterSize - 1];

    for (int i = 0; i < (clusterSize - 1); i++) {
      configuration[i] = replicas.get((id + i + 1) % clusterSize);
    }

    NettyClusterConfig config1 = NettyClusterConfig.from(replicas.get(id % clusterSize), configuration);

    NettyRaftService service1 = NettyRaftService.newBuilder(config1)
        .logDir(logDirectory)
        .timeout(500)
        .transitionListener(groupOfCounters)
        .build(this);

    service1.startAsync().awaitRunning();
    this.service = service1;
  }

  public File makeLogDirectory(File parentDirectory) throws IOException {
    this.logDirectory = new File(parentDirectory, "log" + id);

    if (logDirectory.exists()) {
      Files.delete(logDirectory);
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
    Files.delete(logDirectory);
  }

  /**
   * Wait at most {@code timeout} for this counter to reach value {@code increments}.
   *
   * @param increments value expected for counter.
   * @param timeout    timeout in ms.
   * @throws IllegalStateException if {@code expected} is not reached at end of timeout.
   */
  public void waitForValue(final int increments, long timeout) {
    new Prober(() -> increments == counter).probe(timeout);
  }

  public boolean isLeader() {
    return service.isLeader();
  }

}
