package org.robotninjas.barge.state;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.GetEntriesResult;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;
import org.robotninjas.barge.proto.RaftProto;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.RaftEntry.Entry;
import static org.robotninjas.barge.proto.RaftProto.AppendEntriesResponse;

public class LeaderTest {

  @Test
  public void testReplicaManagerIsRequested() {

    SettableFuture<AppendEntriesResponse> response = SettableFuture.create();

    Client client = mock(Client.class);
    when(client.appendEntries(any(Replica.class), any(RaftProto.AppendEntries.class)))
      .thenReturn(response)
      .thenReturn(Futures.<AppendEntriesResponse>immediateFailedFuture(new Exception()));

    RaftLog log = mock(RaftLog.class);
    Replica self = Replica.fromString("loalhost:10000");
    Replica remote = Replica.fromString("localhost:10001");

    ReplicaManager replicaManager =
      spy(new ReplicaManager(client, log, 0, 10, remote, self));

    replicaManager.fireUpdate();
    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(10, replicaManager.getNextIndex());

    replicaManager.fireUpdate();
    assertTrue(replicaManager.isRunning());
    assertTrue(replicaManager.isRequested());
    assertEquals(10, replicaManager.getNextIndex());

    response.set(
      AppendEntriesResponse.newBuilder()
        .setSuccess(true)
        .setTerm(0)
        .build());

    assertFalse(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(10, replicaManager.getNextIndex());

    verify(replicaManager, times(2)).sendUpdate();

  }

  @Test
  public void testReplicaManagerSuccessfulCall() {

    SettableFuture<AppendEntriesResponse> response = SettableFuture.create();

    Client client = mock(Client.class);
    when(client.appendEntries(any(Replica.class), any(RaftProto.AppendEntries.class)))
      .thenReturn(response);

    RaftLog log = mock(RaftLog.class);
    Entry.Builder entry = Entry.newBuilder().setTerm(0).setCommand(ByteString.EMPTY);
    ImmutableList<Entry> entries = ImmutableList.of(entry.build(), entry.build());
    GetEntriesResult getResult = new GetEntriesResult(0, 0, entries);
//    when(log.getEntriesFrom(anyLong())).thenReturn(getResult);

    Replica self = Replica.fromString("loalhost:10000");
    Replica remote = Replica.fromString("localhost:10001");

    ReplicaManager replicaManager =
      spy(new ReplicaManager(client, log, 0, 10, remote, self));

    replicaManager.fireUpdate();
    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(10, replicaManager.getNextIndex());

    response.set(
      AppendEntriesResponse.newBuilder()
        .setSuccess(true)
        .setTerm(0)
        .build());

    assertFalse(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(12, replicaManager.getNextIndex());

    verify(replicaManager, times(1)).sendUpdate();

  }

  @Test
  public void testReplicaManagerFailedCall() {

    SettableFuture<AppendEntriesResponse> response = SettableFuture.create();

    Client client = mock(Client.class);
    when(client.appendEntries(any(Replica.class), any(RaftProto.AppendEntries.class)))
      .thenReturn(response)
      .thenReturn(Futures.<AppendEntriesResponse>immediateFailedFuture(new Exception()));


    RaftLog log = mock(RaftLog.class);
    Replica self = Replica.fromString("loalhost:10000");
    Replica remote = Replica.fromString("localhost:10001");

    ReplicaManager replicaManager =
      spy(new ReplicaManager(client, log, 0, 10, remote, self));

    replicaManager.fireUpdate();
    assertTrue(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(10, replicaManager.getNextIndex());

    response.set(
      AppendEntriesResponse.newBuilder()
        .setSuccess(false)
        .setTerm(0)
        .build());

    assertFalse(replicaManager.isRunning());
    assertFalse(replicaManager.isRequested());
    assertEquals(9, replicaManager.getNextIndex());

    verify(replicaManager, times(2)).sendUpdate();

  }

}
