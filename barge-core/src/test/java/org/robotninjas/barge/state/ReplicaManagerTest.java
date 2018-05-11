package org.robotninjas.barge.state;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotSame;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.ClusterConfigStub;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.Entry;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;


@SuppressWarnings("unchecked")
public class ReplicaManagerTest {

  private static final ClusterConfig config = ClusterConfigStub.getStub();
  private static final Replica SELF = config.local();
  private static final Replica FOLLOWER = config.getReplica("remote");

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

    CompletableFuture<AppendEntriesResponse> mockResponse = mock(CompletableFuture.class);
    when(mockClient.appendEntries(eq(FOLLOWER), any(AppendEntries.class))).thenReturn(mockResponse);

    GetEntriesResult entriesResult = new GetEntriesResult(0L, 0L, Collections.emptyList());
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockClient, mockRaftLog, FOLLOWER);
    CompletableFuture<AppendEntriesResponse> f1 = replicaManager.requestUpdate();

    AppendEntries appendEntries = AppendEntries.newBuilder()
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

    CompletableFuture<AppendEntriesResponse> f2 = replicaManager.requestUpdate();

    assertNotSame(f1, f2);
    assertTrue(replicaManager.isRunning());
    assertTrue(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

  }

  @Test
  public void testFailedAppend() {

    CompletableFuture<AppendEntriesResponse> response = new CompletableFuture<>();
    when(mockClient.appendEntries(eq(FOLLOWER), any(AppendEntries.class))).thenReturn(response)
      .thenReturn(mock(CompletableFuture.class));


    GetEntriesResult entriesResult = new GetEntriesResult(0L, 0L, Collections.emptyList());
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockClient, mockRaftLog, FOLLOWER);

    replicaManager.requestUpdate();

    AppendEntries appendEntries = AppendEntries.newBuilder()
        .setLeaderId(SELF.toString())
        .setCommitIndex(0)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setTerm(1)
        .build();

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

    AppendEntriesResponse appendEntriesResponse = AppendEntriesResponse.newBuilder()
        .setLastLogIndex(0L)
        .setTerm(1)
        .setSuccess(false)
        .build();

    response.complete(appendEntriesResponse);

    verify(mockClient, times(2)).appendEntries(FOLLOWER, appendEntries);
    verifyNoMoreInteractions(mockClient);

    verify(mockRaftLog, times(2)).getEntriesFrom(1, 1);

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

  }

  @Test
  public void updatesNextIndexBeyondCurrentEntryGivenAppendIsSuccessful() {

    CompletableFuture<AppendEntriesResponse> response = new CompletableFuture<>();
    when(mockClient.appendEntries(eq(FOLLOWER), any(AppendEntries.class))).thenReturn(response)
      .thenReturn(mock(CompletableFuture.class));

    Entry entry = Entry.newBuilder().setTerm(1).setCommand(new byte[0]).build();

    GetEntriesResult entriesResult = new GetEntriesResult(0L, 0L, Lists.newArrayList(entry));
    when(mockRaftLog.getEntriesFrom(anyLong(), anyInt())).thenReturn(entriesResult);

    ReplicaManager replicaManager = new ReplicaManager(mockClient, mockRaftLog, FOLLOWER);

    replicaManager.requestUpdate();

    AppendEntries appendEntries = AppendEntries.newBuilder()
        .setLeaderId(SELF.toString())
        .setCommitIndex(0)
        .setPrevLogIndex(0)
        .setPrevLogTerm(0)
        .setTerm(1)
        .addEntry(entry)
        .build();

    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(1, replicaManager.getNextIndex());

    AppendEntriesResponse appendEntriesResponse = AppendEntriesResponse.newBuilder()
        .setLastLogIndex(0L)
        .setTerm(1)
        .setSuccess(true)
        .build();

    response.complete(appendEntriesResponse);

    verify(mockClient, times(1)).appendEntries(FOLLOWER, appendEntries);
    verifyNoMoreInteractions(mockClient);

    verify(mockRaftLog, times(1)).getEntriesFrom(1, 1);

    assertFalse(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(2, replicaManager.getNextIndex());

  }


}
