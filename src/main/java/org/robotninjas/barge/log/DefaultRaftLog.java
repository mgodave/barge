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
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.LocalReplicaInfo;
import org.robotninjas.barge.rpc.ClientProto;
import org.robotninjas.barge.rpc.RaftProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static journal.io.api.Journal.WriteType;
import static org.robotninjas.barge.rpc.RaftEntry.Entry;

class DefaultRaftLog implements RaftLog {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Journal journal;
  private final LoadingCache<Long, Entry> entries;
  private final SortedMap<Long, EntryMeta> index = new TreeMap<Long, EntryMeta>();
  private final Replica local;
  private volatile long lastLogIndex = -1;

  @Inject
  DefaultRaftLog(Journal journal, @LocalReplicaInfo Replica local) {
    this.local = local;
    this.journal = journal;
    Loader loader = new Loader(index, journal);
    this.entries = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .build(loader);
  }

  public void init() {
    Entry entry = Entry.newBuilder()
      .setTerm(-1L)
      .setCommand(ByteString.EMPTY)
      .build();
    storeEntry(-1L, entry);
  }

  private void storeEntry(long index, Entry entry) {
    try {
      Location loc = journal.write(entry.toByteArray(), WriteType.SYNC);
      EntryMeta meta = new EntryMeta(index, entry.getTerm(), loc);
      this.index.put(index, meta);
      this.entries.put(index, entry);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  public long append(ClientProto.CommitOperation operation, long term) {

    checkState(index.containsKey(lastLogIndex));
    checkState(!index.containsKey(lastLogIndex + 1));

    long index = ++lastLogIndex;

    logger.debug("leader append: index {}, term {}", index, term);

    Entry entry =
      Entry.newBuilder()
        .setCommand(operation.getOp())
        .setTerm(term)
        .build();

    storeEntry(index, entry);

    return index;


  }

  public boolean append(RaftProto.AppendEntries appendEntries) {

    long prevLogTerm = appendEntries.getPrevLogTerm();
    long prevLogIndex = appendEntries.getPrevLogIndex();

    EntryMeta previousEntry = index.get(prevLogIndex);
    if ((previousEntry == null) || (previousEntry.term != prevLogTerm)) {
      return false;
    }

    long index = lastLogIndex + 1;
    logger.debug("follower append: index {}, term {}", index, appendEntries.getTerm());

    this.index.tailMap(prevLogIndex + 1).clear();
    lastLogIndex = appendEntries.getPrevLogIndex();

    for (Entry entry : appendEntries.getEntriesList()) {
      storeEntry(++lastLogIndex, entry);
    }

    return true;

  }

  public GetEntriesResult getEntriesFrom(final long beginningIndex) {
    try {
      checkArgument(beginningIndex >= 0, "index must be >= 0, actual value: %s", beginningIndex);
      Set<Long> indices = index.tailMap(beginningIndex).keySet();
      Iterable<Entry> values = Iterables.transform(indices, new Function<Long, Entry>() {
        @Nullable
        @Override
        public Entry apply(@Nullable Long input) {
          try {
            return entries.get(input);
          } catch (ExecutionException e) {
            e.printStackTrace();
          }
          return null;
        }
      });
      long prevEntryIndex = beginningIndex - 1;
      Entry previousEntry = entries.get(prevEntryIndex);
      long prevEntryTerm = previousEntry.getTerm();
      return new GetEntriesResult(prevEntryTerm, prevEntryIndex, ImmutableList.copyOf(values));
    } catch (ExecutionException e) {
      e.printStackTrace();
      throw propagate(e);
    }
  }

  public long lastLogIndex() {
    return lastLogIndex;
  }

  public long lastLogTerm() {
    return index.get(lastLogIndex()).term;
  }

  public long commitIndex() {
    return 0;
  }

  @Override
  public List<Replica> members() {
    Replica replica1 = Replica.fromString("localhost:10000");
    Replica replica2 = Replica.fromString("localhost:10001");
    Replica replica3 = Replica.fromString("localhost:10002");
    List<Replica> replicas = Lists.newArrayList(replica1, replica2, replica3);
    replicas.remove(local);
    return replicas;
  }

  @Immutable
  class EntryMeta {

    private final long index;
    private final long term;
    private final Location location;

    EntryMeta(long index, long term, Location location) {
      this.index = index;
      this.term = term;
      this.location = location;
    }

  }

  class Loader extends CacheLoader<Long, Entry> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<Long, EntryMeta> index;
    private final Journal journal;

    Loader(Map<Long, EntryMeta> index, Journal journal) {
      this.index = index;
      this.journal = journal;
    }

    @Override
    public Entry load(Long key) throws Exception {
      try {
        logger.debug("Loading {}", key);
        EntryMeta meta = index.get(key);
        Location loc = meta.location;
        byte[] data = journal.read(loc, Journal.ReadType.ASYNC);
        return Entry.parseFrom(data);
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

}

