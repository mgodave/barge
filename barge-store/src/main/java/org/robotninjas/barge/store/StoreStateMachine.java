package org.robotninjas.barge.store;

import com.google.common.collect.Maps;

import org.robotninjas.barge.StateMachine;

import java.nio.ByteBuffer;

import java.util.Map;

import javax.annotation.Nonnull;


/**
 */
public class StoreStateMachine implements StateMachine {

  private final OperationsSerializer operationsSerializer = new OperationsSerializer();
  private final Map<String, byte[]> store = Maps.newConcurrentMap();

  @Override
  public Object applyOperation(@Nonnull ByteBuffer entry) {
    Write write = operationsSerializer.deserialize(entry);

    return store.put(write.getKey(), write.getValue());
  }


  public byte[] get(String key) {
    return store.get(key);
  }
}
