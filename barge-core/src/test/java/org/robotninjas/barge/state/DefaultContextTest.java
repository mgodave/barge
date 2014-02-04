package org.robotninjas.barge.state;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.RaftException;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;
import static org.robotninjas.barge.state.RaftStateContext.StateType;

public class DefaultContextTest {

  private @Mock StateFactory mockStateFactory;
  private @Mock Leader mockLeader;
  private @Mock Follower mockFollower;
  private @Mock Candidate mockCandidate;
  private final AppendEntries appendEntries = AppendEntries.getDefaultInstance();
  private final RequestVote requestVote = RequestVote.getDefaultInstance();

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockStateFactory.candidate()).thenReturn(mockCandidate);
    when(mockStateFactory.leader()).thenReturn(mockLeader);
    when(mockStateFactory.follower()).thenReturn(mockFollower);

  }

  @Test
  public void testDefaultContext() throws RaftException {

    RaftStateContext ctx = new RaftStateContext(mockStateFactory);

    ctx.setState(null, StateType.FOLLOWER);
      
    verify(mockFollower).init(ctx);
    assertEquals(StateType.FOLLOWER, ctx.type());

    ctx.appendEntries(appendEntries);
    verify(mockFollower).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockFollower);

    ctx.requestVote(requestVote);
    verify(mockFollower).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockFollower);


    ctx.setState(mockFollower, StateType.LEADER);
    verify(mockLeader).init(ctx);
    assertEquals(StateType.LEADER, ctx.type());

    ctx.appendEntries(appendEntries);
    verify(mockLeader).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockLeader);

    ctx.requestVote(requestVote);
    verify(mockLeader).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockLeader);


    ctx.setState(mockLeader, StateType.CANDIDATE);
    verify(mockCandidate).init(ctx);
    assertEquals(StateType.CANDIDATE, ctx.type());

    ctx.appendEntries(appendEntries);
    verify(mockCandidate).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockCandidate);

    ctx.requestVote(requestVote);
    verify(mockCandidate).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockCandidate);


  }

}
