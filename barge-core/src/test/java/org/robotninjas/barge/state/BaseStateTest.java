package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;

import javax.annotation.Nonnull;

import static junit.framework.Assert.*;
import static org.mockito.Mockito.when;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;

public class BaseStateTest {

  private final Replica self = Replica.fromString("localhost:8001");
  private final Replica candidate = Replica.fromString("localhost:8000");
  private
  @Mock
  RaftLog mockRaftLog;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.currentTerm()).thenReturn(2l);
    when(mockRaftLog.lastLogIndex()).thenReturn(2l);
    when(mockRaftLog.lastLogTerm()).thenReturn(2l);
    when(mockRaftLog.self()).thenReturn(self);

  }

  @Test
  public void testHaventVoted() {

    BaseState state = new EmptyState();

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.<Replica>absent());
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  @Test
  public void testAlreadyVotedForCandidate() {

    BaseState state = new EmptyState();

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.of(candidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  @Test
  @Ignore
  public void testCandidateWithGreaterTerm() {

    BaseState state = new EmptyState();

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(3)
      .setTerm(2)
      .build();

    Replica otherCandidate = Replica.fromString("localhost:8002");
    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  @Test
  public void testCandidateWithLesserTerm() {

    BaseState state = new EmptyState();

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(1)
      .setTerm(2)
      .build();

    Replica otherCandidate = Replica.fromString("localhost:8002");
    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertFalse(shouldVote);
  }

  @Test
  public void testCandidateWithLesserIndex() {

    BaseState state = new EmptyState();

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(1)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    Replica otherCandidate = Replica.fromString("localhost:8002");
    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertFalse(shouldVote);
  }

  @Test
  @Ignore
  public void testCandidateWithGreaterIndex() {

    BaseState state = new EmptyState();

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(3)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    Replica otherCandidate = Replica.fromString("localhost:8002");
    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  static class EmptyState extends BaseState {
    @Override
    public void init(@Nonnull Context ctx) {

    }

    @Nonnull
    @Override
    public RaftProto.RequestVoteResponse requestVote(@Nonnull Context ctx, @Nonnull RequestVote request) {
      return null;
    }

    @Nonnull
    @Override
    public RaftProto.AppendEntriesResponse appendEntries(@Nonnull Context ctx, @Nonnull RaftProto.AppendEntries request) {
      return null;
    }

    @Nonnull
    @Override
    public ListenableFuture<Boolean> commitOperation(@Nonnull Context ctx, @Nonnull byte[] operation) throws RaftException {
      return null;
    }

  }

}
