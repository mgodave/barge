package org.robotninjas.barge.log;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.LogProto;
import org.robotninjas.barge.proto.RaftEntry;

import java.io.IOException;
import java.util.SortedMap;

import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.Throwables.propagate;
import static journal.io.api.Journal.ReadType;
import static journal.io.api.Journal.WriteType;

class RaftJournal {

  private final Journal journal;
  private final SortedMap<Long, Location> entryIndex = Maps.newTreeMap();

  public RaftJournal(journal.io.api.Journal journal) {
    this.journal = journal;
  }

  public void init() {

    try {
      for (Location location : journal.redo()) {
        LogProto.JournalEntry entry = read(location);
        if (entry.hasAppend()) {
          LogProto.Append append = entry.getAppend();
          long index = append.getIndex();
          long term = append.getEntry().getTerm();
          entryIndex.put(index, location);
        }
      }
    } catch (IOException e) {
      propagate(e);
    }

  }

  private LogProto.JournalEntry read(Location loc) {
    try {
      byte[] data = journal.read(loc, ReadType.SYNC);
      return LogProto.JournalEntry.parseFrom(data);
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  private void delete(Location loc) {
    try {
      journal.delete(loc);
    } catch (IOException e) {
      propagate(e);
    }
  }

  private Location write(LogProto.JournalEntry entry) {
    try {
      return journal.write(entry.toByteArray(), WriteType.SYNC);
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public RaftEntry.Entry get(long index) {
    return read(entryIndex.get(index)).getAppend().getEntry();
  }

  public void removeAfter(long index) {

    Iterable<Location> entries =
      entryIndex.tailMap(index + 1).values();

    for (Location loc : entries) {
      delete(loc);
    }

    entryIndex.headMap(index).clear();

  }

  public void appendEntry(RaftEntry.Entry entry, long index) {
    LogProto.JournalEntry je =
      LogProto.JournalEntry.newBuilder()
        .setAppend(LogProto.Append.newBuilder()
          .setEntry(entry)
          .setIndex(index))
        .build();

    Location location = write(je);
    entryIndex.put(index, location);
  }

  public void appendTerm(long term) {
    write(LogProto.JournalEntry.newBuilder()
      .setTerm(LogProto.Term.newBuilder()
        .setTerm(term))
      .build());
  }

  public void appendCommit(long commit) {
    write(LogProto.JournalEntry.newBuilder()
      .setCommit(LogProto.Commit.newBuilder()
        .setIndex(commit))
      .build());
  }

  public void appendVote(Optional<Replica> vote) {
    write(LogProto.JournalEntry.newBuilder()
      .setVote(LogProto.Vote.newBuilder()
        .setVotedFor(vote.transform(toStringFunction()).or("")))
      .build());
  }

  public void replay(Visitor visitor) {

    try {
      for (Location location : journal.redo()) {

        LogProto.JournalEntry entry = read(location);

        if (entry.hasAppend()) {
          LogProto.Append append = entry.getAppend();
          visitor.append(append.getEntry(), append.getIndex());
        } else if (entry.hasCommit()) {
          LogProto.Commit commit = entry.getCommit();
          visitor.commit(commit.getIndex());
        } else if (entry.hasTerm()) {
          LogProto.Term term = entry.getTerm();
          visitor.term(term.getTerm());
        } else if (entry.hasVote()) {
          LogProto.Vote vote = entry.getVote();
          String votedfor = vote.getVotedFor();
          Replica replica = votedfor == null
            ? null : Replica.fromString(votedfor);
          visitor.vote(Optional.fromNullable(replica));
        }

      }
    } catch (IOException e) {
      propagate(e);
    }

  }

  static interface Visitor {

    void term(long term);

    void vote(Optional<Replica> vote);

    void commit(long commit);

    void append(RaftEntry.Entry entry, long index);

  }

}
