package org.robotninjas.barge.state;

import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.RequestVote;

import javax.annotation.Nonnull;

/**
 * A listener which is notified of protocol messages received by a {@link org.robotninjas.barge.state.RaftStateContext}
 * instance.
 */
public interface RaftProtocolListener {

  /**
   * Called when {@code raft} instance is initialized.
   * <p>
   *   This method is called <em>after</em> instance has been initialized. 
   * </p>
   * @param raft the raft instance.
   */
  void init(Raft raft);

  /**
   * Called after append entries has been received and processed by {@code raft} instance.
   * 
   * @param raft the raft instance receiving RPC.
   * @param entries the appended entries.
   */
  void appendEntries(@Nonnull Raft raft, @Nonnull AppendEntries entries);

  /**
   * Called after request vote has been received and processed by {@code raft} instance.
   * 
   * @param raft the raft instance receiving RPC.
   * @param vote the vote request.
   */
  void requestVote(@Nonnull Raft raft, @Nonnull RequestVote vote);

  /**
   * Called after commit request has been received and processed by {@code raft} instance.
   * @param raft the raft instance receiving RPC.
   * @param operation bytes to be committed to underlying state machine. Maybe empty.
   */
  void commit(@Nonnull Raft raft, @Nonnull byte[] operation);
}
