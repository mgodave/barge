package org.robotninjas.barge.state;

import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.LogModule;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;
import org.robotninjas.barge.proto.RaftProto;

import javax.annotation.Nonnull;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.Context.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Context.StateType.FOLLOWER;

public class CandidateTest {

  private @Mock ScheduledExecutorService mockScheduler;
  private @Mock Replica mockReplica;
  private @Mock Client mockRaftClient;
  private @Mock StateFactory mockStateFactory;
  private RaftLog raftLog;

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockRaftClient.requestVote(any(Replica.class), any(RequestVote.class)))
      .thenReturn(Futures.<RequestVoteResponse>immediateFailedFuture(new Exception("")));

    Injector injector = Guice.createInjector(new LogModule(Files.createTempDir(), null));
    raftLog = injector.getInstance(RaftLog.class);

    Candidate mockCandidate = mock(Candidate.class);
    when(mockStateFactory.candidate()).thenReturn(mockCandidate);

    Follower mockFollower = mock(Follower.class);
    when(mockStateFactory.follower()).thenReturn(mockFollower);

    Leader mockLeader = mock(Leader.class);
    when(mockStateFactory.leader()).thenReturn(mockLeader);

    ScheduledFuture mockScheduledFuture = mock(ScheduledFuture.class);
    when(mockScheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(mockScheduledFuture);
  }

  @Test
  public void testRequestVoteWithNewerTerm() throws Exception {

    Candidate candidate = new Candidate(raftLog, mockScheduler, 1, mockRaftClient);
    Context context = new DefaultContext(mockStateFactory);
    candidate.init(context);

    Replica mockCandidate = Replica.fromString("localhost:10001");

    RequestVote request =
      RequestVote.newBuilder()
        .setCandidateId(mockCandidate.toString())
        .setLastLogIndex(1L)
        .setLastLogTerm(1L)
        .setTerm(2L)
        .build();

    RequestVoteResponse response = candidate.requestVote(context, request);

    assertTrue(response.getVoteGranted());
    //assertEquals(2, response.getTerm());
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

    Candidate candidate = new Candidate(raftLog, mockScheduler, 1, mockRaftClient);
    Context context = new DefaultContext(mockStateFactory);
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

    Candidate candidate = new Candidate(raftLog, mockScheduler, 1, mockRaftClient);
    Context context = new DefaultContext(mockStateFactory);
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

    Candidate candidate = new Candidate(raftLog, mockScheduler, 1, mockRaftClient);
    Context context = new DefaultContext(mockStateFactory);
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

//  @Test
//  public void testCheckElected() {
//
//    Function<Integer, Boolean> isElected;
//
//    isElected = IsElectedFunction.isElected();
//    assertFalse(isElected.apply(0));
//    assertTrue(isElected.apply(1));
//
//    isElected = IsElectedFunction.isElected();
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertTrue(isElected.apply(2));
//
//    isElected = IsElectedFunction.isElected();
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertTrue(isElected.apply(2));
//    assertTrue(isElected.apply(3));
//
//    isElected = IsElectedFunction.isElected();
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertFalse(isElected.apply(2));
//    assertTrue(isElected.apply(3));
//    assertTrue(isElected.apply(4));
//
//    isElected = IsElectedFunction.isElected();
//    assertFalse(isElected.apply(0));
//    assertFalse(isElected.apply(1));
//    assertFalse(isElected.apply(2));
//    assertTrue(isElected.apply(3));
//    assertTrue(isElected.apply(4));
//    assertTrue(isElected.apply(5));
//
//  }

  @Nonnull
  RequestVoteResponse rvr(boolean outcome) {
    return RequestVoteResponse.newBuilder()
      .setTerm(1L)
      .setVoteGranted(outcome)
      .build();
  }

  @Nonnull
  RequestVote rv(long term, long index) {
    return RequestVote.newBuilder()
      .setCandidateId("candidate")
      .setLastLogIndex(index)
      .setLastLogTerm(term)
      .setTerm(1L)
      .build();
  }

//  @Test
//  public void testShouldVoteFor() {
//
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
//
//  }

}
