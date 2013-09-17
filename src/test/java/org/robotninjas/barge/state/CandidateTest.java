package org.robotninjas.barge.state;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.context.RaftContext;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RaftProto;
import org.robotninjas.barge.rpc.RpcClientProvider;

import java.util.concurrent.ScheduledExecutorService;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.robotninjas.barge.rpc.RaftProto.*;
import static org.robotninjas.barge.state.Context.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Context.StateType.FOLLOWER;

public class CandidateTest {

  @Mock
  private ScheduledExecutorService mockScheduler;
  @Mock
  private Replica mockReplica;
  @Mock
  private RaftClient mockRaftClient;
  @Mock
  private RpcClientProvider mockClientProvider;
  @Mock
  private RaftLog mockRaftLog;
  @Mock
  private StateFactory mockStateFactory;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftClient.requestVote(any(RequestVote.class)))
      .thenReturn(Futures.<RequestVoteResponse>immediateFailedFuture(new Exception("")));

    when(mockClientProvider.get(mockReplica)).thenReturn(mockRaftClient);

    when(mockRaftLog.append(any(RaftProto.AppendEntries.class))).thenReturn(true);
    when(mockRaftLog.lastLogIndex()).thenReturn(1L);
    when(mockRaftLog.lastLogTerm()).thenReturn(1L);
    when(mockRaftLog.members()).thenReturn(Lists.newArrayList(mockReplica));

  }

  @Test
  public void testInit() throws Exception {

  }

  @Test
  public void testRequestVoteWithNewerTerm() throws Exception {

    RaftContext raftContext = new RaftContext(mockRaftLog, mockReplica);
    Candidate candidate = new Candidate(raftContext, mockScheduler, 1, mockClientProvider);
    Context context = new Context(mockStateFactory);
    candidate.init(context);

    Replica mockCandidate = mock(Replica.class);
    when(mockCandidate.toString()).thenReturn("candidate");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(2L)
        .build();

    RequestVoteResponse response = candidate.requestVote(context, request);

    assertTrue(response.getVoteGranted());
    assertEquals(2, response.getTerm());
    assertEquals(FOLLOWER, context.getState());

  }

  @Test
  public void testRequestVoteWithOlderTerm() throws Exception {

  }

  @Test
  public void testRequestVoteWithSameTerm() throws Exception {

  }

  @Test
  public void testAppendEntriesWithNewerTerm() throws Exception {

    RaftContext raftContext = new RaftContext(mockRaftLog, mockReplica);
    Candidate candidate = new Candidate(raftContext, mockScheduler, 1, mockClientProvider);
    Context context = new Context(mockStateFactory);
    candidate.init(context);

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(2L)
        .setLeaderId("leader")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    AppendEntriesResponse response = candidate.appendEntries(context, request);

    assertTrue(response.getSuccess());
    assertEquals(2, response.getTerm());
    assertEquals(FOLLOWER, context.getState());
  }

  @Test
  public void testAppendEntriesWithOlderTerm() throws Exception {

    RaftContext raftContext = new RaftContext(mockRaftLog, mockReplica);
    Candidate candidate = new Candidate(raftContext, mockScheduler, 1, mockClientProvider);
    Context context = new Context(mockStateFactory);
    context.setState(CANDIDATE);
    candidate.init(context);

    AppendEntriesResponse response;

    AppendEntries request =
      RaftProto.AppendEntries.newBuilder()
        .setTerm(0L)
        .setLeaderId("leader")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    response = candidate.appendEntries(context, request);

    assertFalse(response.getSuccess());
    assertEquals(1, response.getTerm());
    assertEquals(CANDIDATE, context.getState());

  }

  @Test
  public void testAppendEntriesWithSameTerm() throws Exception {

    RaftContext raftContext = new RaftContext(mockRaftLog, mockReplica);
    Candidate candidate = new Candidate(raftContext, mockScheduler, 1, mockClientProvider);
    Context context = new Context(mockStateFactory);
    candidate.init(context);

    AppendEntries request =
      AppendEntries.newBuilder()
        .setTerm(1L)
        .setLeaderId("leader")
        .setPrevLogIndex(1L)
        .setPrevLogTerm(1L)
        .setCommitIndex(1L)
        .build();

    AppendEntriesResponse response = candidate.appendEntries(context, request);

    assertTrue(response.getSuccess());
    assertEquals(1, response.getTerm());
    assertEquals(FOLLOWER, context.getState());

  }

  @Test
  public void testCheckElected() {
//
//    Function<Integer, Boolean> isElected;
//
//    isElected = Candidate.isElected(1);
//    assertFalse(isElected.apply(0));
//    assertTrue(isElected.apply(1));
//
//    isElected = Candidate.isElected(2);
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertTrue(isElected.apply(2));
//
//    isElected = Candidate.isElected(3);
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertTrue(isElected.apply(2));
//    assertTrue(isElected.apply(3));
//
//    isElected = Candidate.isElected(4);
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertFalse(isElected.apply(2));
//    assertTrue(isElected.apply(3));
//    assertTrue(isElected.apply(4));
//
//    isElected = Candidate.isElected(5);
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertFalse(isElected.apply(2));
//    assertTrue(isElected.apply(3));
//    assertTrue(isElected.apply(4));
//    assertTrue(isElected.apply(5));

  }

  RequestVoteResponse rvr(boolean outcome) {
    return RequestVoteResponse.newBuilder()
      .setTerm(1L)
      .setVoteGranted(outcome)
      .build();
  }

  @Test
  public void testCountVotes() {
//
//    Function<List<RequestVoteResponse>, Integer> countVotes = Candidate.countVotes();
//
//    List<RequestVoteResponse> responses;
//
//    responses = Lists.newArrayList(rvr(true), rvr(true));
//    assertEquals(new Integer(3), countVotes.apply(responses));
//
//    responses = Lists.newArrayList(rvr(false), rvr(false));
//    assertEquals(new Integer(1), countVotes.apply(responses));
//
//    responses = Lists.newArrayList(null, null);
//    assertEquals(new Integer(1), countVotes.apply(responses));
//
//    responses = Lists.newArrayList(null, rvr(true));
//    assertEquals(new Integer(2), countVotes.apply(responses));
//
//    responses = Lists.newArrayList(null, rvr(false));
//    assertEquals(new Integer(1), countVotes.apply(responses));
  }

  RequestVote rv(long term, long index) {
    return RequestVote.newBuilder()
      .setCandidateId("candidate")
      .setLastLogIndex(index)
      .setLastLogTerm(term)
      .setTerm(1L)
      .build();
  }

  @Test
  public void testShouldVoteFor() {

//    RaftContext raftContext = new RaftContext(mockRaftLog, mockReplica);
//
//    assertFalse(Candidate.shouldVoteFor(raftContext, rv(1L, 1L)));
//    assertTrue(Candidate.shouldVoteFor(raftContext, rv(1L, 2L)));
//    assertFalse(Candidate.shouldVoteFor(raftContext, rv(1L, 0L)));
//    assertTrue(Candidate.shouldVoteFor(raftContext, rv(2L, 2L)));
//    assertTrue(Candidate.shouldVoteFor(raftContext, rv(2L, 0L)));
//    assertTrue(Candidate.shouldVoteFor(raftContext, rv(2L, 1L)));
//    assertFalse(Candidate.shouldVoteFor(raftContext, rv(0L, 2L)));
//    assertFalse(Candidate.shouldVoteFor(raftContext, rv(0L, 0L)));
//    assertFalse(Candidate.shouldVoteFor(raftContext, rv(0L, 1L)));

  }

}
