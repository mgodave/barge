package org.robotninjas.barge.state;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;

import java.util.Collections;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.RaftEntry.Entry;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.AppendEntriesResponse;

public class ReplicaManagerTest {

  private static final Replica SELF = Replica.fromString("localhost:8080");
  private static final Replica FOLLOWER = Replica.fromString("localhost:1000");

  private @Mock Client mockClient;
  private @Mock RaftLog mockRaftLog;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.self()).thenReturn(SELF);
    when(mockRaftLog.lastLogTerm()).thenReturn(0L);
    when(mockRaftLog.lastLogIndex()).thenReturn(0L);
    when(mockRaftLog.currentTerm()).thenReturn(1L);

  }

  @Test
  public void testInitialSendOutstanding() {

    ListenableFuture<AppendEntriesResponse> mockResponse = mock(ListenableFuture.class);
    when(mockClient.appendEntries(eq(FOLLOWER), any(AppendEntries.class))).thenReturn(mockResponse);

    GetEntriesResult entriesResult = new GetEntriesResult(0l, 0l, Collections.<Entry>emptyList());
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockClient, mockRaftLog, FOLLOWER);
    ListenableFuture f1 = replicaManager.fireUpdate();

    AppendEntries appendEntries =
      AppendEntries.newBuilder()
        .setLeaderId(SELF.toString())
        .setCommitIndex(0)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setTerm(1)
        .build();

    verify(mockClient, times(1)).appendEntries(FOLLOWER, appendEntries);
    verifyNoMoreInteractions(mockClient);

    verify(mockRaftLog, times(1)).getEntriesFrom(1, 1);

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

    ListenableFuture f2 = replicaManager.fireUpdate();

    assertNotSame(f1, f2);
    assertTrue(replicaManager.isRunning());
    assertTrue(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

  }



}
