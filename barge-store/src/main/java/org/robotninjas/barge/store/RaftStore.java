package org.robotninjas.barge.store;

import com.google.common.base.Optional;


/**
 */
public interface RaftStore {

  /**
   * Writes some key/value pair to this store.
   *
   * <p>This operations writes some value under some key in this store. It expects the underlying Raft instance to
   * be a leader or to point to a leader, and waits for completion of operation to the cluster of raft instances
   * to complete.</p>
   *
   * @param write operation to apply to store.
   * @return the old value.
   */
  byte[] write(Write write);

  /**
   * Reads current value of some key if it exists.
   *
   * @param key key to read data from.
   * @return a byte array containing raw data for this key or {@link com.google.common.base.Optional#absent()} if
   * no value is associated with given key in this store.
   */
  Optional<byte[]> read(String key);
}
