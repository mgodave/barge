package org.robotninjas.barge.store;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.guava.api.Assertions;

import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.robotninjas.barge.state.Raft;

import static java.nio.ByteBuffer.wrap;


public class RaftStoreInstanceTest {

  private static final byte[] oldValue = new byte[] { 0x41 };
  private static final Write write = new Write("foo", new byte[] { 0x42 });

  private final Raft raft = mock(Raft.class);
  private final OperationsSerializer serializer = new OperationsSerializer();

  private final RaftStoreInstance raftStoreInstance = new RaftStoreInstance(raft);

  @Test
  public void commitSerializedWriteOperationToRaftThenReturnOldValueWhenWritingKeyValuePair() throws Exception {
    when(raft.commitOperation(serializer.serialize(write))).thenReturn(Futures.<Object>immediateFuture(oldValue));

    assertThat(raftStoreInstance.write(write)).isEqualTo(oldValue);
  }

  @Test
  public void mapsKeyToValueWhenApplyingWriteOperation() throws Exception {
    raftStoreInstance.applyOperation(wrap(serializer.serialize(write)));

    Optional<byte[]> read = raftStoreInstance.read("foo");

    Assertions.assertThat(read).isPresent();
    assertThat(read.get()).isEqualTo(new byte[] { 0x42 });
  }
}
