package org.robotninjas.barge.store;

import com.google.common.base.Optional;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Throwables.propagate;

import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.state.Raft;

import java.util.concurrent.ExecutionException;

import javax.inject.Inject;


/**
 */
public class RaftStoreInstance implements RaftStore {

  private final Raft raft;
  private final OperationsSerializer operationsSerializer = new OperationsSerializer();

  private final StoreStateMachine storeStateMachine;

  @Inject
  public RaftStoreInstance(Raft raft, StoreStateMachine storeStateMachine) {
    this.raft = raft;
    this.storeStateMachine = storeStateMachine;
  }

  @Override
  public byte[] write(Write write) {

    try {
      return (byte[]) raft.commitOperation(operationsSerializer.serialize(write)).get();
    } catch (RaftException | InterruptedException | ExecutionException e) {
      throw propagate(e);
    }
  }

  @Override
  public Optional<byte[]> read(String key) {
    return fromNullable(storeStateMachine.get(key));
  }


}
