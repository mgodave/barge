package org.robotninjas.barge;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * An instance of a set of replica managed through Raft protocol.
 * <p>
 *   A {@link RaftService} instance is constructed by specific builders depending on: Communication protocol used,
 *   persistent storage, network characteristics...
 * </p>
 */
public interface RaftService {

  /**
   * Asynchronously executes an operation on the state machine managed by barge.
   * <p>
   * When the result becomes available, the operation is guaranteed to have been committed to the Raft
   * cluster in such a way that it is permanent and will be seen, eventually, by all present and
   * future members of the cluster.
   * </p>
   *
   * @param operation an arbitrary operation to be sent to the <em>state machine</em> managed by Raft.
   * @return the result of executing the operation, wrapped in a {@link ListenableFuture}, that can be retrieved
   *         at a later point in time.
   * @throws org.robotninjas.barge.RaftException
   */
  ListenableFuture<Object> commitAsync(byte[] operation) throws RaftException;

  /**
   * Synchronously executes and operation on the state machine managed by barge.
   * <p>
   * This method is semantically equivalent to:
   * </p>
   * <pre>
   *     commitAsync(op).get();
   * </pre>
   *
   * @param operation an arbitrary operation to be sent to the <em>state machine</em> managed by Raft.
   * @return the result of executing the operation, as returned by the state machine.
   * @throws RaftException
   * @throws InterruptedException if current thread is interrupted while waiting for the result to be available.
   */
  Object commit(byte[] operation) throws RaftException, InterruptedException;
}
