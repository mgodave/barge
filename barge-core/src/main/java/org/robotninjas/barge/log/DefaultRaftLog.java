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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.robotninjas.barge.ClusterMembers;
import org.robotninjas.barge.LocalReplicaInfo;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.LogProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static journal.io.api.Journal.ReadType.ASYNC;
import static journal.io.api.Journal.WriteType;
import static org.robotninjas.barge.proto.LogProto.JournalEntry;
import static org.robotninjas.barge.proto.RaftEntry.Entry;
import static org.robotninjas.barge.proto.RaftProto.AppendEntries;

@NotThreadSafe
class DefaultRaftLog implements RaftLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRaftLog.class);
  private static final Entry SENTINEL_ENTRY = Entry.newBuilder().setCommand(ByteString.EMPTY).setTerm(0).build();

  private final Journal journal;
  private final SortedMap<Long, EntryMeta> entryIndex = new TreeMap<Long, EntryMeta>();
  private final Replica local;
  private final List<Replica> members;
  private final StateMachineProxy stateMachine;
  private volatile long lastLogIndex = 0;
  private volatile long currentTerm = 0;
  private volatile Optional<Replica> votedFor = Optional.absent();
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  @Inject
  DefaultRaftLog(@Nonnull Journal journal,
                 @LocalReplicaInfo @Nonnull Replica local,
                 @ClusterMembers @Nonnull List<Replica> members,
                 @Nonnull StateMachineProxy stateMachine) {

    this.local = checkNotNull(local);
    this.journal = checkNotNull(journal);
    this.members = checkNotNull(members);
    this.stateMachine = checkNotNull(stateMachine);

  }

  @Override
  public void load() {
    this.entryIndex.put(0L, new EntryMeta(0, 0, null));

    LOGGER.info("Replaying log");
    try {
      for (Location loc : journal.redo()) {

        byte[] data = journal.read(loc, Journal.ReadType.SYNC);
        JournalEntry journalEntry = JournalEntry.parseFrom(data);

        if (journalEntry.hasAppend()) {

          LogProto.Append append = journalEntry.getAppend();

          long index = append.getIndex();
          LOGGER.debug("Append {}", append.getEntry());
          Entry entry = append.getEntry();
          long term = entry.getTerm();

          EntryMeta meta = new EntryMeta(index, term, loc);
          this.entryIndex.put(index, meta);

          this.lastLogIndex = Math.max(lastLogIndex, index);

        }

        if (journalEntry.hasTerm()) {

          LogProto.Term term = journalEntry.getTerm();
          LOGGER.debug("Term {}", term);
          this.currentTerm = Math.max(currentTerm, term.getTerm());

        }

        if (journalEntry.hasVote()) {

          LogProto.Vote vote = journalEntry.getVote();
          if (vote.hasVotedFor()) {
            Replica candidate = Replica.fromString(vote.getVotedFor());
            votedFor = Optional.of(candidate);
          } else {
            votedFor = Optional.absent();
          }
          LOGGER.debug("Vote {}", votedFor.orNull());

        }

        if (journalEntry.hasCommit()) {

          LogProto.Commit commit = journalEntry.getCommit();
          this.commitIndex = Math.max(commit.getIndex(), commitIndex);
          LOGGER.debug("Commit {}", commit.getIndex());

        }

        LOGGER.debug("lastLogIndex {}, currentTerm {}, commitIndex {}",
          lastLogIndex, currentTerm, commitIndex);


        fireComitted();

      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    LOGGER.info("Finished replaying log lastIndex {}, currentTerm {}, commitIndex {}, votedFor {}",
      lastLogIndex, currentTerm, commitIndex, votedFor.orNull());
  }

  private void storeEntry(long index, @Nonnull Entry entry) {
    try {

      LOGGER.debug("{}", entry);

      JournalEntry journalEntry =
        JournalEntry.newBuilder()
          .setAppend(LogProto.Append.newBuilder()
            .setIndex(index)
            .setEntry(entry))
          .build();

      Location loc = journal.write(journalEntry.toByteArray(), WriteType.SYNC);
      EntryMeta meta = new EntryMeta(index, entry.getTerm(), loc);
      this.entryIndex.put(index, meta);
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  private Entry loadEntry(long index) {

    try {
      EntryMeta meta = entryIndex.get(index);
      byte[] data = journal.read(meta.location, ASYNC);
      JournalEntry entry = JournalEntry.parseFrom(data);
      return entry.getAppend().getEntry();
    } catch (Exception e) {
      e.printStackTrace();
      throw propagate(e);
    }

  }

  private Function<Long, Entry> loadEntryFunc() {
    return new Function<Long, Entry>() {
      @Nullable
      @Override
      public Entry apply(@Nullable Long input) {
        return loadEntry(input);
      }
    };
  }

  public long append(@Nonnull byte[] operation) {

    checkState(entryIndex.containsKey(lastLogIndex));
    checkState(!entryIndex.containsKey(lastLogIndex + 1));

    long index = ++lastLogIndex;

//    LOGGER.debug("leader append: index {}, term {}", index, currentTerm);

    Entry entry =
      Entry.newBuilder()
        .setCommand(ByteString.copyFrom(operation))
        .setTerm(currentTerm)
        .build();

    storeEntry(index, entry);

    return index;


  }

  public boolean append(@Nonnull AppendEntries appendEntries) {

    final long prevLogIndex = appendEntries.getPrevLogIndex();
    final long prevLogTerm = appendEntries.getPrevLogTerm();
    final List<Entry> entries = appendEntries.getEntriesList();

    EntryMeta previousEntry = entryIndex.get(prevLogIndex);
    if ((previousEntry == null) || (previousEntry.term != prevLogTerm)) {
      LOGGER.debug("Append prevLogIndex {} prevLogTerm {} previousEntry {}", prevLogIndex, prevLogTerm, previousEntry);
      return false;
    }

    SortedMap<Long, EntryMeta> old = this.entryIndex.tailMap(prevLogIndex + 1);
    for (EntryMeta e : old.values()) {
      try {
        LOGGER.debug("Deleting {}", e.index);
        journal.delete(e.location);
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }
    old.clear();
    lastLogIndex = prevLogIndex;

    for (Entry entry : entries) {
      storeEntry(++lastLogIndex, entry);
    }

    return true;

  }

  @Nonnull
  public GetEntriesResult getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max) {

    checkArgument(beginningIndex >= 0);

    Set<Long> indices = entryIndex.tailMap(beginningIndex).keySet();
    Iterable<Entry> values = Iterables.transform(limit(indices, max), loadEntryFunc());

    long previousIndex = beginningIndex - 1;
    Entry previousEntry = (previousIndex <= 0) ? SENTINEL_ENTRY : loadEntry(previousIndex);

    GetEntriesResult result = new GetEntriesResult(previousEntry.getTerm(), previousIndex, newArrayList(values));

    return result;

  }

  void fireComitted() {
    try {
      for (long i = lastApplied + 1; i <= Math.min(commitIndex, lastLogIndex); ++i, ++lastApplied) {
        byte[] rawCommand = loadEntry(i).getCommand().toByteArray();
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
    return entryIndex.get(lastLogIndex()).term;
  }

  public long commitIndex() {
    return commitIndex;
  }

  public void updateCommitIndex(long index) {

    commitIndex = index;

    try {
      JournalEntry entry =
        JournalEntry.newBuilder()
          .setCommit(LogProto.Commit.newBuilder()
            .setIndex(index))
          .build();

      journal.write(entry.toByteArray(), WriteType.SYNC);
    } catch (IOException e) {
      Throwables.propagate(e);
    }

    fireComitted();

  }

  @Nonnull
  @Override
  public List<Replica> members() {
    return Collections.unmodifiableList(members);
  }

  public long currentTerm() {
    return currentTerm;
  }

  public void updateCurrentTerm(@Nonnegative long term) {

    checkArgument(term >= 0);

    MDC.put("term", Long.toString(term));
    LOGGER.debug("New term {}", term);

    currentTerm = term;

    try {
      JournalEntry entry =
        JournalEntry.newBuilder()
          .setTerm(LogProto.Term.newBuilder()
            .setTerm(term))
          .build();
      journal.write(entry.toByteArray(), WriteType.SYNC);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Nonnull
  public Optional<Replica> lastVotedFor() {
    return votedFor;
  }

  public void updateVotedFor(@Nonnull Optional<Replica> vote) {

    LOGGER.debug("Voting for {}", vote.orNull());

    votedFor = vote;

    try {
      LogProto.Vote.Builder voteBuilder =
        LogProto.Vote.newBuilder();

      if (vote.isPresent()) {
        voteBuilder.setVotedFor(vote.get().toString());
      }

      JournalEntry entry =
        JournalEntry.newBuilder()
          .setVote(voteBuilder)
          .build();

      journal.write(entry.toByteArray(), WriteType.SYNC);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Nonnull
  public Replica self() {
    return local;
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

  @Immutable
  static final class EntryMeta {

    private final long index;
    private final long term;
    private final Location location;

    EntryMeta(long index, long term, @Nonnull Location location) {
      this.index = index;
      this.term = term;
      this.location = location;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(getClass())
        .add("index", index)
        .add("term", term)
        .toString();
    }
  }

}
