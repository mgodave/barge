package org.robotninjas.barge.state;

public interface StateFactory {

  Candidate candidate();

  Leader leader();

  Follower follower();

}
