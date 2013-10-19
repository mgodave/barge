package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.proto.RaftProto;
import org.robotninjas.barge.rpc.Client;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;
import static org.robotninjas.barge.state.Context.StateType.FOLLOWER;

public class CandidateTest {

  private final long term = 2L;
  private final Replica self = Replica.fromString("localhost:1000");

  private @Mock ScheduledExecutorService mockScheduler;
  private @Mock Replica mockReplica;
  private @Mock Client mockRaftClient;
  private @Mock StateFactory mockStateFactory;
  private @Mock StateMachine mockStateMachine;
  private @Mock RaftLog mockRaftLog;
  private @Mock Context mockContext;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.self()).thenReturn(self);
    when(mockRaftLog.lastVotedFor()).thenReturn(Optional.<Replica>absent());
    when(mockRaftLog.lastLogTerm()).thenReturn(0L);
    when(mockRaftLog.lastLogIndex()).thenReturn(0L);
    when(mockRaftLog.currentTerm()).thenReturn(term);

    ScheduledFuture mockScheduledFuture = mock(ScheduledFuture.class);
    when(mockScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
      .thenReturn(mockScheduledFuture);

  }

  @Test
  public void testRequestVoteWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 150, mockRaftClient);
    candidate.init(mockContext);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(4L)
        .build();

    candidate.requestVote(mockContext, request);

    verify(mockRaftLog).updateCurrentTerm(3L);
    verify(mockRaftLog).updateCurrentTerm(4L);
    verify(mockRaftLog, times(2)).updateCurrentTerm(anyLong());

    verify(mockRaftLog).updateVotedFor(Optional.of(self));
    verify(mockRaftLog).updateVotedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(2)).updateVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).updateCommitIndex(anyLong());

    verify(mockContext, times(1)).setState(FOLLOWER);
    verifyNoMoreInteractions(mockContext);

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testRequestVoteWithOlderTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 150, mockRaftClient);
    candidate.init(mockContext);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(1L)
        .build();

    candidate.requestVote(mockContext, request);

    verify(mockRaftLog).updateCurrentTerm(3L);
    verify(mockRaftLog, times(1)).updateCurrentTerm(anyLong());

    verify(mockRaftLog).updateVotedFor(Optional.of(self));
    verify(mockRaftLog, never()).updateVotedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(1)).updateVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).updateCommitIndex(anyLong());

    verifyZeroInteractions(mockContext);

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testRequestVoteWithSameTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 150, mockRaftClient);
    candidate.init(mockContext);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(2L)
        .build();

    candidate.requestVote(mockContext, request);

    verify(mockRaftLog).updateCurrentTerm(3L);
    verify(mockRaftLog, times(1)).updateCurrentTerm(anyLong());

    verify(mockRaftLog).updateVotedFor(Optional.of(self));
    verify(mockRaftLog, never()).updateVotedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(1)).updateVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).updateCommitIndex(anyLong());

    verify(mockContext, never()).setState(any(Context.StateType.class));

    verifyZeroInteractions(mockContext);

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testAppendEntriesWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 1, mockRaftClient);
    candidate.init(mockContext);

    Replica mockLeader = Replica.fromString("localhost:10001");

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(4L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockContext, request);

    verify(mockRaftLog).updateCurrentTerm(3L);
    verify(mockRaftLog).updateCurrentTerm(4L);
    verify(mockRaftLog, times(2)).updateCurrentTerm(anyLong());

    verify(mockRaftLog).updateVotedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).updateVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).updateCommitIndex(anyLong());

    verify(mockContext).setState(FOLLOWER);
    verifyNoMoreInteractions(mockContext);

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithOlderTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 1, mockRaftClient);
    candidate.init(mockContext);

    Replica mockLeader = Replica.fromString("localhost:10001");

    AppendEntries request =
      RaftProto.AppendEntries.newBuilder()
        .setTerm(1L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockContext, request);

    verify(mockRaftLog).updateCurrentTerm(3L);
    verify(mockRaftLog, times(1)).updateCurrentTerm(anyLong());

    verify(mockRaftLog).updateVotedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).updateVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).updateCommitIndex(anyLong());

    verifyZeroInteractions(mockContext);

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithSameTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 1, mockRaftClient);
    candidate.init(mockContext);

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(2L)
        .setLeaderId("leader")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockContext, request);

    verify(mockRaftLog).updateCurrentTerm(3L);
    verify(mockRaftLog, times(1)).updateCurrentTerm(anyLong());

    verify(mockRaftLog).updateVotedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).updateVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).updateCommitIndex(anyLong());

    verify(mockContext).setState(FOLLOWER);
    verifyNoMoreInteractions(mockContext);

    verifyZeroInteractions(mockRaftClient);

  }

}
