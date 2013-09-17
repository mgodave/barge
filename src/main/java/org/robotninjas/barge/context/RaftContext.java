package org.robotninjas.barge.context;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.LocalReplicaInfo;
import org.robotninjas.barge.log.RaftLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class RaftContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftContext.class);

  private final RaftLog log;
  private final Replica self;
  private long term = 0;
  private Optional<Replica> votedFor = Optional.absent();
  private long commitIndex = 0;

  @Inject
  public RaftContext(RaftLog log, @LocalReplicaInfo Replica self) {
    MDC.put("term", Long.toString(term));
    this.log = log;
    this.self = self;
  }

  public long term() {
    return term;
  }

  public void term(long term) {
    MDC.put("term", Long.toString(term));
    LOGGER.debug("New term {}", term);
    this.term = term;
  }

  public Optional<Replica> votedFor() {
    return votedFor;
  }

  public void votedFor(Optional<Replica> vote) {
    LOGGER.debug("Voting for {}", vote.orNull());
    this.votedFor = votedFor();
  }

  public RaftLog log() {
    return log;
  }

  public Replica self() {
    return self;
  }

  public long commitIndex() {
    return commitIndex;
  }
}
