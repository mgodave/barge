/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
