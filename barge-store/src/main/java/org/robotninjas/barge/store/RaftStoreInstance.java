package org.robotninjas.barge.store;

import com.google.common.base.Optional;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Throwables.propagate;
import com.google.common.collect.Maps;

import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.state.Raft;

import java.nio.ByteBuffer;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import javax.inject.Inject;


/**
 */
public class RaftStoreInstance implements RaftStore, StateMachine {

  private final Raft raft;
  private final OperationsSerializer operationsSerializer = new OperationsSerializer();
  private final Map<String, byte[]> store = Maps.newConcurrentMap();

  @Inject
  public RaftStoreInstance(Raft raft) {
    this.raft = raft;
  }

  @Override
  public byte[] write(Write write) {

    try {
      return (byte[]) raft.commitOperation(operationsSerializer.serialize(write)).get();
    } catch (RaftException e) {
      throw propagate(e);
    } catch (InterruptedException e) {
      throw propagate(e);
    } catch (ExecutionException e) {
      throw propagate(e);
    }
  }

  @Override
  public Optional<byte[]> read(String key) {
    return fromNullable(store.get(key));
  }

  @Override
  public Object applyOperation(@Nonnull ByteBuffer entry) {
    Write write = operationsSerializer.deserialize(entry.array());

    return store.put(write.getKey(), write.getValue());
  }


}
