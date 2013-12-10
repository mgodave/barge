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

package org.robotninjas.barge.log;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import journal.io.api.Journal;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.Replica;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static org.robotninjas.barge.proto.RaftEntry.Entry;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;

@NotThreadSafe
class DefaultRaftLog implements RaftLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftLog.class);

  private final ClusterConfig config;
  private final StateMachineProxy stateMachine;
  private volatile long lastLogIndex = 0;
  private volatile long currentTerm = 0;
  private volatile Optional<Replica> votedFor = Optional.absent();
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  private final RaftJournal journal;
  private final TreeMap<Long, Entry> logEntries = Maps.newTreeMap();

  @Inject
  DefaultRaftLog(@Nonnull Journal journal, @Nonnull ClusterConfig config, @Nonnull StateMachineProxy stateMachine) {

    this.config = checkNotNull(config);
    this.journal = new RaftJournal(journal);
    this.stateMachine = checkNotNull(stateMachine);

  }

  @Override
  public void init() {

    LOGGER.info("Replaying log");

    journal.init();

    journal.replay(new RaftJournal.Visitor() {
      @Override
      public void term(long term) {
        currentTerm = Math.max(currentTerm, term);
      }

      @Override
      public void vote(Optional<Replica> vote) {
        votedFor = vote;
      }

      @Override
      public void commit(long commit) {
        commitIndex = Math.max(commitIndex, commit);
      }

      @Override
      public void append(Entry entry, long index) {
        lastLogIndex = Math.max(lastLogIndex, index);
        logEntries.put(index, entry);
      }
    });

    fireComitted();

    LOGGER.info("Finished replaying log lastIndex {}, currentTerm {}, commitIndex {}, votedFor {}",
      lastLogIndex, currentTerm, commitIndex, votedFor.orNull());
  }



  public long append(@Nonnull byte[] operation) {

    long index = ++lastLogIndex;

    Entry entry =
      Entry.newBuilder()
        .setCommand(ByteString.copyFrom(operation))
        .setTerm(currentTerm)
        .build();

    journal.appendEntry(entry, index);

    return index;


  }

  public boolean append(@Nonnull AppendEntries appendEntries) {

    final long prevLogIndex = appendEntries.getPrevLogIndex();
    final long prevLogTerm = appendEntries.getPrevLogTerm();
    final List<Entry> entries = appendEntries.getEntriesList();

    if (!logEntries.containsKey(prevLogIndex) ||
      (logEntries.get(prevLogIndex).getTerm() != prevLogTerm)) {
      LOGGER.debug("Append prevLogIndex {} prevLogTerm {}", prevLogIndex, prevLogTerm);
      return false;
    }

    journal.removeAfter(prevLogIndex + 1);
    logEntries.tailMap(prevLogIndex + 1).clear();

    lastLogIndex = prevLogIndex;

    for (Entry entry : entries) {
      journal.appendEntry(entry, ++lastLogIndex);
    }

    return true;

  }

  @Nonnull
  public EntrySet getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max) {

    checkArgument(beginningIndex >= 0);

    Entry previousEntry = journal.get(beginningIndex - 1);
    Iterable<Entry> entries = logEntries.tailMap(beginningIndex).values();

    return new EntrySet(previousEntry.getTerm(), beginningIndex - 1, entries);

  }

  void fireComitted() {
    try {
      for (long i = lastApplied + 1; i <= Math.min(commitIndex, lastLogIndex); ++i, ++lastApplied) {
        byte[] rawCommand = journal.get(i).getCommand().toByteArray();
        final ByteBuffer operation = ByteBuffer.wrap(rawCommand).asReadOnlyBuffer();
        stateMachine.dispatchOperation(operation);
      }
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  public long lastLogIndex() {
    return lastLogIndex;
  }

  public long lastLogTerm() {
    return journal.get(lastLogIndex()).getTerm();
  }

  public long commitIndex() {
    return commitIndex;
  }

  public void updateCommitIndex(long index) {
    journal.appendCommit((commitIndex = index));
    fireComitted();
  }

  @Nonnull
  @Override
  public List<Replica> members() {
    return Lists.newArrayList(config.remote());
  }

  public long currentTerm() {
    return currentTerm;
  }

  public void updateCurrentTerm(@Nonnegative long term) {

    checkArgument(term >= 0);

    MDC.put("term", Long.toString(term));
    LOGGER.debug("New term {}", term);

    journal.appendTerm((currentTerm = term));

  }

  @Nonnull
  public Optional<Replica> lastVotedFor() {
    return votedFor;
  }

  public void updateVotedFor(@Nonnull Optional<Replica> vote) {
    LOGGER.debug("Voting for {}", vote.orNull());
    journal.appendVote((votedFor = vote));
  }

  @Nonnull
  public Replica self() {
    return config.local();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
      .add("lastLogIndex", lastLogIndex)
      .add("lastApplied", lastApplied)
      .add("commitIndex", commitIndex)
      .add("votedFor", votedFor)
      .toString();
  }



}
