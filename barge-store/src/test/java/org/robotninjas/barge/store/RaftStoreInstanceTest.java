package org.robotninjas.barge.store;

import com.google.common.util.concurrent.Futures;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.guava.api.Assertions;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.robotninjas.barge.state.Raft;


public class RaftStoreInstanceTest {

  private static final byte[] oldValue = new byte[] { 0x41 };
  private static final Write write = new Write("foo", new byte[] { 0x42 });

  private final Raft raft = mock(Raft.class);
  private final StoreStateMachine storeStateMachine = mock(StoreStateMachine.class);

  private final OperationsSerializer serializer = new OperationsSerializer();

  private final RaftStoreInstance raftStoreInstance = new RaftStoreInstance(raft, storeStateMachine);

  @Test
  public void commitSerializedWriteOperationToRaftThenReturnOldValueWhenWritingKeyValuePair() throws Exception {
    when(raft.commitOperation(serializer.serialize(write))).thenReturn(Futures.<Object>immediateFuture(Futures.immediateFuture(oldValue)));

    assertThat(raftStoreInstance.write(write)).isEqualTo(oldValue);
  }

  @Test
  public void whenReadingReturnsAbsentOptionalIfKeyIsNotPresentInStore() throws Exception {
    when(storeStateMachine.get("foo")).thenReturn(null);

    Assertions.assertThat(raftStoreInstance.read("foo")).isAbsent();
  }
}
