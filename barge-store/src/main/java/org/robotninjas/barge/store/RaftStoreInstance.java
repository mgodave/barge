package org.robotninjas.barge.store;

import com.google.common.base.Optional;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Throwables.propagate;
import com.google.common.util.concurrent.ListenableFuture;

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

      // TODO this is ugly...
      return ((ListenableFuture<byte[]>) raft.commitOperation(operationsSerializer.serialize(write)).get()).get();
    } catch (RaftException | InterruptedException | ExecutionException e) {
      throw propagate(e);
    }
  }

  @Override
  public Optional<byte[]> read(String key) {
    return fromNullable(storeStateMachine.get(key));
  }


}
