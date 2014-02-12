package org.robotninjas.barge;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.robotninjas.barge.state.Raft;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import javax.annotation.Nonnull;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

public class StartStopTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartStopTest.class);

  private File TEST_TMP_DIR;

  private final NettyReplica replicas[] = new NettyReplica[] { NettyReplica.fromString("localhost:10001") };

  private List<Server> allServers = Lists.newArrayList();

  class Server {

    final NettyRaftService raftService;
    final File logDirectory;
    final ServerState state;

    public Server(NettyRaftService raftService, File logDirectory, ServerState state) {
      this.raftService = raftService;
      this.logDirectory = logDirectory;
      this.state = state;
    }

    public void start() {
      LOGGER.info("Starting server");
      raftService.startAsync().awaitRunning();
    }

    public void stop() {
      LOGGER.info("Stopping server");
      raftService.stopAsync().awaitTerminated();
    }

    public void setState(String s) throws RaftException, InterruptedException {
      raftService.commit(s.getBytes(Charsets.UTF_8));
    }

    public String getState() {
      ByteBuffer bb = state.getState();
      byte[] v = new byte[bb.remaining()];
      bb.get(v);
      return new String(v, Charsets.UTF_8);
    }

    public boolean isLeader() {
      return raftService.isLeader();
    }

  }

  Server buildServer(int id) {
    File logDir = new File(TEST_TMP_DIR, "log" + id);

    assertThat(logDir.exists() || logDir.mkdirs()).isTrue();

    ServerState state = new ServerState();

    NettyClusterConfig clusterConfig = NettyClusterConfig.from(replicas[0]);

    NettyRaftService raftService = NettyRaftService.newBuilder(clusterConfig).logDir(logDir).timeout(500).build(state);

    Server server = new Server(raftService, logDir, state);
    allServers.add(server);
    return server;
  }

  @Test(timeout = 5000)
  public void canStartAndStop() throws Exception {
    {
      Server server = buildServer(1);

      server.start();

      while (!server.isLeader()) {
        Thread.sleep(50);
      }

      server.setState("A");

      server.stop();
    }

    {
      Server server = buildServer(1);

      server.start();

      while (!server.isLeader()) {
        Thread.sleep(50);
      }

      assertThat(server.getState()).isEqualTo("A");

      server.stop();
    }
  }

  @Before
  public void prepare() throws Exception {
    TEST_TMP_DIR = Files.createTempDir();
  }

  @After
  public void cleanup() throws Exception {
    for (Server server : allServers) {
      switch (server.raftService.state()) {
      case RUNNING:
      case STOPPING:
      case STARTING:
        server.stop();
        break;
      }
    }
    SimpleCounterMachine.delete(TEST_TMP_DIR);
  }

  public static class ServerState implements StateMachine {

    private ByteBuffer state;

    @Override
    public Object applyOperation(@Nonnull ByteBuffer entry) {
      this.state = entry;
      return 0;
    }

    public ByteBuffer getState() {
      return state.duplicate();
    }
  }
}
