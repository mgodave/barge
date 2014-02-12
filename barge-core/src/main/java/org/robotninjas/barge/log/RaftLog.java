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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import journal.io.api.Journal;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.unmodifiableList;

@NotThreadSafe
public class RaftLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftLog.class);
  private static final byte[] EMPTY = new byte[0];
  private static final Entry SENTINEL = Entry.newBuilder().setCommand(EMPTY).setTerm(0).build();

  private final TreeMap<Long, RaftJournal.Mark> log = Maps.newTreeMap();
  private final ClusterConfig config;
  private final StateMachineProxy stateMachine;
  private final RaftJournal journal;

  private final ConcurrentMap<Object, SettableFuture<Object>> operationResults = Maps.newConcurrentMap();

  private volatile long lastLogIndex = 0;
  private volatile long lastLogTerm = 0;
  private volatile long currentTerm = 0;
  private volatile Optional<Replica> votedFor = Optional.absent();
  private volatile long commitIndex = 0;
  private volatile long lastApplied = 0;

  @Inject
  RaftLog(@Nonnull Journal journal, @Nonnull ClusterConfig config,
          @Nonnull StateMachineProxy stateMachine) {
    this.journal = new RaftJournal(checkNotNull(journal), checkNotNull(config));
    this.config = checkNotNull(config);
    this.stateMachine = checkNotNull(stateMachine);
  }

  public void load() {

    LOGGER.info("Replaying log");

    journal.replay(new RaftJournal.Visitor() {
      @Override
      public void term(RaftJournal.Mark mark, long term) {
        currentTerm = Math.max(currentTerm, term);
      }

      @Override
      public void vote(RaftJournal.Mark mark, Optional<Replica> vote) {
        votedFor = vote;
      }

      @Override
      public void commit(RaftJournal.Mark mark, long commit) {
        commitIndex = Math.max(commitIndex, commit);
      }

      @Override
      public void append(RaftJournal.Mark mark, Entry entry, long index) {
        lastLogIndex = Math.max(index, lastLogIndex);
        lastLogTerm = Math.max(entry.getTerm(), lastLogTerm);
        log.put(index, mark);
      }
    });

    fireComitted();

    LOGGER.info("Finished replaying log lastIndex {}, currentTerm {}, commitIndex {}, lastVotedFor {}",
        lastLogIndex, currentTerm, commitIndex, votedFor.orNull());
  }

  private SettableFuture<Object> storeEntry(final long index, @Nonnull Entry entry) {
    LOGGER.debug("{} storing {}", config.local(), entry);
    RaftJournal.Mark mark = journal.appendEntry(entry, index);
    log.put(index, mark);
    SettableFuture<Object> result = SettableFuture.create();
    operationResults.put(index, result);
    return result;
  }

  public ListenableFuture<Object> append(@Nonnull byte[] operation) {

    long index = ++lastLogIndex;
    lastLogTerm = currentTerm;

    Entry entry =
        Entry.newBuilder()
            .setCommand(operation)
            .setTerm(currentTerm)
            .build();

    return storeEntry(index, entry);

  }

  public boolean append(@Nonnull AppendEntries appendEntries) {

    final long prevLogIndex = appendEntries.getPrevLogIndex();
    final long prevLogTerm = appendEntries.getPrevLogTerm();
    final List<Entry> entries = appendEntries.getEntriesList();

    if (log.containsKey(prevLogIndex)) {

      RaftJournal.Mark previousMark = log.get(prevLogIndex);
      Entry previousEntry = journal.get(previousMark);

      if ((prevLogIndex > 0) && previousEntry.getTerm() != prevLogTerm) {
        LOGGER.debug("Append prevLogIndex {} prevLogTerm {}", prevLogIndex, prevLogTerm);
        return false;
      }

      journal.truncateTail(previousMark);
      log.tailMap(prevLogIndex, false).clear();

    }

    lastLogIndex = prevLogIndex;
    for (Entry entry : entries) {
      storeEntry(++lastLogIndex, entry);
      lastLogTerm = entry.getTerm();
    }

    return true;

  }

  @Nonnull
  public GetEntriesResult getEntriesFrom(@Nonnegative long beginningIndex, @Nonnegative int max) {

    checkArgument(beginningIndex >= 0);

    long previousIndex = beginningIndex - 1;
    Entry previous = previousIndex <= 0 ? SENTINEL : journal.get(log.get(previousIndex));
    Iterable<Entry> entries = FluentIterable
        .from(log.tailMap(beginningIndex).values())
        .limit(max)
        .transform(new Function<RaftJournal.Mark, Entry>() {
          @Nullable
          @Override
          public Entry apply(@Nullable RaftJournal.Mark input) {
            return journal.get(input);
          }
        });

    return new GetEntriesResult(previous.getTerm(), previousIndex, entries);

  }

  void fireComitted() {
    try {
      for (long i = lastApplied + 1; i <= Math.min(commitIndex, lastLogIndex); ++i, ++lastApplied) {
        Entry entry = journal.get(log.get(i));
        byte[] rawCommand = entry.getCommand();
        final ByteBuffer operation = ByteBuffer.wrap(rawCommand).asReadOnlyBuffer();
        ListenableFuture<Object> result = stateMachine.dispatchOperation(operation);

        final SettableFuture<Object> returnedResult = operationResults.remove(i);
        // returnedResult may be null on log replay
        if (returnedResult != null) {
          Futures.addCallback(result, new PromiseBridge<Object>(returnedResult));
        }
      }
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  public long lastLogIndex() {
    return lastLogIndex;
  }

  public long lastLogTerm() {
    return lastLogTerm;
  }

  public long commitIndex() {
    return commitIndex;
  }

  public ClusterConfig config() {
    return config;
  }

  public void commitIndex(long index) {
    commitIndex = index;
    journal.appendCommit(index);
    fireComitted();
  }

  public long currentTerm() {
    return currentTerm;
  }

  public void currentTerm(@Nonnegative long term) {
    checkArgument(term >= 0);
    MDC.put("term", Long.toString(term));
    LOGGER.debug("New term {}", term);
    currentTerm = term;
    votedFor = Optional.absent();
    journal.appendTerm(term);
  }

  @Nonnull
  public Optional<Replica> votedFor() {
    return votedFor;
  }

  public void votedFor(@Nonnull Optional<Replica> vote) {
    LOGGER.debug("Voting for {}", vote.orNull());
    votedFor = vote;
    journal.appendVote(vote);
  }

  @Nonnull
  public Replica self() {
    return config.local();
  }

  @Nonnull
  public List<Replica> members() {
    return unmodifiableList(newArrayList(config.remote()));
  }

  @Nonnull
  public Replica getReplica(String info) {
    return config.getReplica(info);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass())
        .add("lastLogIndex", lastLogIndex)
        .add("lastApplied", lastApplied)
        .add("commitIndex", commitIndex)
        .add("lastVotedFor", votedFor)
        .toString();
  }

  private static class PromiseBridge<V> implements FutureCallback<V> {

    private final SettableFuture<V> promise;

    private PromiseBridge(SettableFuture<V> promise) {
      this.promise = promise;
    }

    @Override
    public void onSuccess(@Nullable V result) {
      promise.set(result);
    }

    @Override
    public void onFailure(Throwable t) {
      promise.setException(t);
    }
  }

}
