package org.robotninjas.barge.store;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import static java.nio.ByteBuffer.wrap;


public class StoreStateMachineTest {


  private static final Write write = new Write("foo", new byte[] { 0x42 });

  private final OperationsSerializer serializer = new OperationsSerializer();

  private final StoreStateMachine raftStoreInstance = new StoreStateMachine();

  @Test
  public void mapsKeyToValueWhenApplyingWriteOperation() throws Exception {
    raftStoreInstance.applyOperation(wrap(serializer.serialize(write)));

    byte[] read = raftStoreInstance.get("foo");

    assertThat(read).isEqualTo(new byte[] { 0x42 });
  }

  @Test
  public void applyingWriteReturnsOldValueGivenKeyIsAlreadyMapped() throws Exception {
    raftStoreInstance.applyOperation(wrap(serializer.serialize(write)));

    byte[] old = (byte[]) raftStoreInstance.applyOperation(wrap(serializer.serialize(new Write("foo", new byte[] { 0x43 }))));

    assertThat(old).isEqualTo(new byte[] { 0x42 });
  }

  @Test
  public void applyingWriteReturnsNullValueGivenKeyIsNotMapped() throws Exception {
    byte[] old = (byte[]) raftStoreInstance.applyOperation(wrap(serializer.serialize(new Write("foo", new byte[] { 0x43 }))));

    assertThat(old).isNull();
  }

}
