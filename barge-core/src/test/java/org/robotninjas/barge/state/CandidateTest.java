package org.robotninjas.barge.state;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.FiberStub;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.ClusterConfigStub;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;

import java.util.concurrent.ScheduledFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.LEADER;

public class CandidateTest {

  private final long term = 2L;

  private Fiber scheduler = new FiberStub();
  private @Mock Replica mockReplica;
  private final ClusterConfig config = ClusterConfigStub.getStub();
  private final Replica self = config.local();
  private @Mock Client mockRaftClient;
  private @Mock StateMachine mockStateMachine;
  private @Mock RaftLog mockRaftLog;
  private @Mock RaftStateContext mockRaftStateContext;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftLog.self()).thenReturn(self);
    when(mockRaftLog.votedFor()).thenReturn(Optional.<Replica>absent());
    when(mockRaftLog.lastLogTerm()).thenReturn(0L);
    when(mockRaftLog.lastLogIndex()).thenReturn(0L);
    when(mockRaftLog.currentTerm()).thenReturn(term);
    when(mockRaftLog.config()).thenReturn(config);
    when(mockRaftLog.getReplica(anyString())).thenAnswer(new Answer<Replica>() {
      @Override
      public Replica answer(InvocationOnMock invocation) throws Throwable {
        String arg = (String) invocation.getArguments()[0];
        return config.getReplica(arg);
      }
    });

    ScheduledFuture mockScheduledFuture = mock(ScheduledFuture.class);

    when(mockRaftStateContext.type()).thenReturn(CANDIDATE);

    RequestVoteResponse response = RequestVoteResponse.newBuilder().setTerm(0).setVoteGranted(true).build();
    ListenableFuture<RequestVoteResponse> responseFuture = Futures.immediateFuture(response);
    when(mockRaftClient.requestVote(any(Replica.class), any(RequestVote.class))).thenReturn(responseFuture);
  }

  @Test
  public void testRequestVoteWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, scheduler, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = config.getReplica("other");

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

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog).votedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(2)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext, times(1)).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext, times(1)).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testRequestVoteWithOlderTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, scheduler, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = config.getReplica("other");

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

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, never()).votedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testRequestVoteWithSameTerm() throws Exception {
    Candidate candidate = new Candidate(mockRaftLog, scheduler, 150, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockCandidate = config.getReplica("other");

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

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(Optional.of(mockCandidate));
    verify(mockRaftLog, times(2)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(State.class), any(RaftStateContext.StateType.class));

    verifyZeroInteractions(mockRaftStateContext);

    verifyZeroInteractions(mockRaftClient);
  }

  @Test
  public void testAppendEntriesWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, scheduler, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockLeader = config.getReplica("other");

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

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, times(1)).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.FOLLOWER));
    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithOlderTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, scheduler, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    Replica mockLeader = config.getReplica("other");

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(1L)
        .setLeaderId(mockLeader.toString())
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, never()).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(Raft.StateType.LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

  @Test
  public void testAppendEntriesWithSameTerm() throws Exception {

    Candidate candidate = new Candidate(mockRaftLog, scheduler, 1, mockRaftClient);
    candidate.init(mockRaftStateContext);

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(2L)
        .setLeaderId("leader:1000")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    candidate.appendEntries(mockRaftStateContext, request);

    verify(mockRaftLog).currentTerm(3L);
    verify(mockRaftLog, times(1)).currentTerm(anyLong());

    verify(mockRaftLog).votedFor(Optional.of(self));
    verify(mockRaftLog, times(1)).votedFor(any(Optional.class));

    verify(mockRaftLog, times(1)).commitIndex(anyLong());

    verify(mockRaftStateContext).setState(any(Candidate.class), eq(LEADER));

    verifyZeroInteractions(mockRaftClient);

  }

}
