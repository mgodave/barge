package org.robotninjas.barge.state;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.robotninjas.barge.RaftException;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static org.robotninjas.barge.proto.ClientProto.CommitOperation;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;
import static org.robotninjas.barge.proto.RaftProto.RequestVote;
import static org.robotninjas.barge.state.Context.StateType;

public class DefaultContextTest {

  private @Mock StateFactory mockStateFactory;
  private @Mock Leader mockLeader;
  private @Mock Follower mockFollower;
  private @Mock Candidate mockCandidate;
  private final AppendEntries appendEntries = AppendEntries.getDefaultInstance();
  private final RequestVote requestVote = RequestVote.getDefaultInstance();
  private final CommitOperation commitOperation = CommitOperation.getDefaultInstance();

  @Before
  public void initMocks() {

    MockitoAnnotations.initMocks(this);

    when(mockStateFactory.candidate()).thenReturn(mockCandidate);
    when(mockStateFactory.leader()).thenReturn(mockLeader);
    when(mockStateFactory.follower()).thenReturn(mockFollower);

  }

  @Test
  public void testDefaultContext() throws RaftException {

    DefaultContext ctx = new DefaultContext(mockStateFactory);

    ctx.init();
    verify(mockFollower).init(ctx);
    assertEquals(StateType.FOLLOWER, ctx.getState());

    ctx.appendEntries(appendEntries);
    verify(mockFollower).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockFollower);

    ctx.requestVote(requestVote);
    verify(mockFollower).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockFollower);

    ctx.commitOperation(commitOperation);
    verify(mockFollower).commitOperation(ctx, commitOperation);
    verifyNoMoreInteractions(mockFollower);


    ctx.setState(StateType.LEADER);
    verify(mockLeader).init(ctx);
    assertEquals(StateType.LEADER, ctx.getState());

    ctx.appendEntries(appendEntries);
    verify(mockLeader).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockLeader);

    ctx.requestVote(requestVote);
    verify(mockLeader).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockLeader);

    ctx.commitOperation(commitOperation);
    verify(mockLeader).commitOperation(ctx, commitOperation);
    verifyNoMoreInteractions(mockLeader);


    ctx.setState(StateType.CANDIDATE);
    verify(mockCandidate).init(ctx);
    assertEquals(StateType.CANDIDATE, ctx.getState());

    ctx.appendEntries(appendEntries);
    verify(mockCandidate).appendEntries(ctx, appendEntries);
    verifyNoMoreInteractions(mockCandidate);

    ctx.requestVote(requestVote);
    verify(mockCandidate).requestVote(ctx, requestVote);
    verifyNoMoreInteractions(mockCandidate);

    ctx.commitOperation(commitOperation);
    verify(mockCandidate).commitOperation(ctx, commitOperation);
    verifyNoMoreInteractions(mockCandidate);

  }

}
