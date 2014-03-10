package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.ClusterConfigStub;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;

import javax.annotation.Nonnull;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class BaseStateTest {

  private final ClusterConfig config = ClusterConfigStub.getStub();
  private final Replica self = config.local();
  private final Replica candidate = config.getReplica("candidate");
  private @Mock RaftLog mockRaftLog;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.currentTerm()).thenReturn(2l);
    when(mockRaftLog.lastLogIndex()).thenReturn(2l);
    when(mockRaftLog.lastLogTerm()).thenReturn(2l);
    when(mockRaftLog.self()).thenReturn(self);
    when(mockRaftLog.config()).thenReturn(config);
    when(mockRaftLog.getReplica(anyString())).thenAnswer(new Answer<Replica>() {
      @Override
      public Replica answer(InvocationOnMock invocation) throws Throwable {
        String arg = (String) invocation.getArguments()[0];
        return config.getReplica(arg);
      }
    });

  }

  @Test
  public void testHaventVoted() {

    BaseState state = new EmptyState(mockRaftLog);

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    when(mockRaftLog.votedFor()).thenReturn(Optional.<Replica>absent());
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  @Test
  public void testAlreadyVotedForCandidate() {

    BaseState state = new EmptyState(mockRaftLog);

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    when(mockRaftLog.votedFor()).thenReturn(Optional.of(candidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  @Test
  @Ignore
  public void testCandidateWithGreaterTerm() {

    BaseState state = new EmptyState(mockRaftLog);

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(3)
      .setTerm(2)
      .build();

    Replica otherCandidate = config.getReplica("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  @Test
  public void testCandidateWithLesserTerm() {

    BaseState state = new EmptyState(mockRaftLog);

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(2)
      .setLastLogTerm(1)
      .setTerm(2)
      .build();

    Replica otherCandidate = config.getReplica("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertFalse(shouldVote);
  }

  @Test
  public void testCandidateWithLesserIndex() {

    BaseState state = new EmptyState(mockRaftLog);

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(1)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    Replica otherCandidate = config.getReplica("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertFalse(shouldVote);
  }

  @Test
  @Ignore
  public void testCandidateWithGreaterIndex() {

    BaseState state = new EmptyState(mockRaftLog);

    RequestVote requestVote = RequestVote.newBuilder()
      .setCandidateId(candidate.toString())
      .setLastLogIndex(3)
      .setLastLogTerm(2)
      .setTerm(2)
      .build();

    Replica otherCandidate = config.getReplica("other");
    when(mockRaftLog.votedFor()).thenReturn(Optional.of(otherCandidate));
    boolean shouldVote = state.shouldVoteFor(mockRaftLog, requestVote);

    assertTrue(shouldVote);
  }

  static class EmptyState extends BaseState {
    protected EmptyState(RaftLog log) {
      super(null, log);
    }

    @Override
    public void init(@Nonnull RaftStateContext ctx) {

    }

    @Override
    public void destroy(@Nonnull RaftStateContext ctx) {

    }

    @Nonnull
    @Override
    public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {
      return null;
    }

    @Nonnull
    @Override
    public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {
      return null;
    }

    @Nonnull
    @Override
    public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
      return null;
    }

  }

}
