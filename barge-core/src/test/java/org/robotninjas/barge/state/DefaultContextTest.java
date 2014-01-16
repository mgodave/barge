package org.robotninjas.barge.state;

import com.google.common.base.Optional;
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

    verify(mockFollower).init(ctx, Optional.absent());
    assertEquals(StateType.FOLLOWER, ctx.getState());

    ctx.appendEntries(appendEntries);
    verify(mockFollower).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockFollower);

    ctx.requestVote(requestVote);
    verify(mockFollower).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockFollower);


    ctx.setState(StateType.LEADER);
    verify(mockLeader).init(ctx, Optional.absent());
    assertEquals(StateType.LEADER, ctx.getState());

    ctx.appendEntries(appendEntries);
    verify(mockLeader).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockLeader);

    ctx.requestVote(requestVote);
    verify(mockLeader).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockLeader);


    ctx.setState(StateType.CANDIDATE);
    verify(mockCandidate).init(ctx, Optional.absent());
    assertEquals(StateType.CANDIDATE, ctx.getState());

    ctx.appendEntries(appendEntries);
    verify(mockCandidate).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockCandidate);

    ctx.requestVote(requestVote);
    verify(mockCandidate).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockCandidate);


  }

}
