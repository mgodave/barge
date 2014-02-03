package org.robotninjas.barge;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

public class NettyRaftServiceTest {

  private static final File target = new File(System.getProperty("basedir", "."), "target");

  @Rule
  public GroupOfCounters counters = new GroupOfCounters(3, target);

  @Test(timeout = 30000)
  public void canRun3RaftInstancesReachingCommonState() throws Exception {
    // TODO replace sleep with observation of leader election transition
    Thread.sleep(3000);

    int increments = 10;

    for (int i = 0; i < increments; i++) {
      counters.commitToLeader(new byte[]{1});
    }

    counters.waitAllToReachValue(increments, 10000);
  }


}
