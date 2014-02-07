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

public class CandidateTest {

  private final long term = 2L;
  private final Replica self = Replica.fromString("localhost:1000");

  private @Mock ScheduledExecutorService mockScheduler;
  private @Mock Replica mockReplica;
  private @Mock Client mockRaftClient;
  private @Mock StateMachine mockStateMachine;
  private @Mock RaftLog mockRaftLog;
  private @Mock
  RaftStateContext mockRaftStateContext;

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
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(4L)
        .build();

    candidate.requestVote(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog).currentTerm(4L);
    verify(mockRaftLog, times(2)).currentTerm(anyLong());

    verify(mockRaftLog).lastVotedFor(Optional.of(self));
    verify(mockRaftLog).lastVotedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(2)).lastVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext, times(1)).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext, times(1)).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testRequestVoteWithOlderTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(1L)
        .build();

    candidate.requestVote(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).lastVotedFor(Optional.of(self));
    verify(mockRaftLog, never()).lastVotedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(1)).lastVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testRequestVoteWithSameTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(2L)
        .build();

    candidate.requestVote(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).lastVotedFor(Optional.of(self));
    verify(mockRaftLog, never()).lastVotedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(1)).lastVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(State.class), any(RaftStateContext.StateType.class));

    verifyZeroInteractions(mockRaftStateContext);

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testAppendEntriesWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockLeader = Replica.fromString("localhost:10001");

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(4L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog).currentTerm(4L);
    verify(mockRaftLog, times(2)).currentTerm(anyLong());

    verify(mockRaftLog).lastVotedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).lastVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithOlderTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockLeader = Replica.fromString("localhost:10001");

    AppendEntries request =
      RaftProto.AppendEntries.newBuilder()
        .setTerm(1L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).lastVotedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).lastVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithSameTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, mockScheduler, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(2L)
        .setLeaderId("leader")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).lastVotedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).lastVotedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

}
