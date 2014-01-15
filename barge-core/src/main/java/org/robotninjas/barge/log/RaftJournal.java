package org.robotninjas.barge.log;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.LogProto;
import org.robotninjas.barge.proto.RaftEntry;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.TreeMap;

import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.Throwables.propagate;
import static journal.io.api.Journal.ReadType;
import static journal.io.api.Journal.WriteType;

class RaftJournal {

  private final Journal journal;
  private final TreeMap<Long, Location> entryIndex = Maps.newTreeMap();

  public RaftJournal(Journal journal) {
    this.journal = journal;
  }

  public boolean isEmpty() {
    return journal.getFiles().isEmpty();
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

  public RaftEntry.Entry get(Mark mark) {
    LogProto.JournalEntry entry = read(mark.getLocation());
    return entry.getAppend().getEntry();
  }

  public void removeAfter(long index) {

    Collection<Location> entries =
      entryIndex.tailMap(index, false).values();

    for (Location loc : entries) {
      delete(loc);
    }

    entries.clear();

  }

  public void removeBefore(long index) {

    Collection<Location> entries =
      entryIndex.headMap(index, false).values();

    for (Location loc : entries) {
      delete(loc);
    }

    entries.clear();

  }

  public Mark mark() {
    try {
      return new Mark(journal.undo().iterator().next());
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public void truncateHead(Mark mark) {
    try {
      for (Location loc : journal.undo(mark.getLocation())) {
        delete(loc);
      }
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public void truncateTail(Mark mark) {
    try {
      for (Location loc : journal.redo(mark.getLocation())) {
        delete(loc);
      }
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public Mark appendEntry(RaftEntry.Entry entry, long index) {
    LogProto.JournalEntry je =
      LogProto.JournalEntry.newBuilder()
        .setAppend(LogProto.Append.newBuilder()
          .setEntry(entry)
          .setIndex(index))
        .build();

    Location location = write(je);
    entryIndex.put(index, location);
    return new Mark(location);
  }

  public Mark appendTerm(long term) {
    Location location =
      write(LogProto.JournalEntry.newBuilder()
        .setTerm(LogProto.Term.newBuilder()
          .setTerm(term))
        .build());
    return new Mark(location);
  }

  public Mark appendCommit(long commit) {
    Location location =
      write(LogProto.JournalEntry.newBuilder()
        .setCommit(LogProto.Commit.newBuilder()
          .setIndex(commit))
        .build());
    return new Mark(location);
  }

  public Mark appendVote(Optional<Replica> vote) {
    Location location =
      write(LogProto.JournalEntry.newBuilder()
        .setVote(LogProto.Vote.newBuilder()
          .setVotedFor(vote.transform(toStringFunction()).or("")))
        .build());
    return new Mark(location);
  }

  public Mark appendSnapshot(File file, long index, long term) {
    Location location =
      write(LogProto.JournalEntry.newBuilder()
        .setSnapshot(LogProto.Snapshot.newBuilder()
          .setLastIncludedIndex(index)
          .setLastIncludedTerm(term)
          .setSnapshotFile(file.getName()))
        .build());
    return new Mark(location);
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

  static class Mark {

    private final Location location;

    private Mark(Location location) {
      this.location = location;
    }

    private Location getLocation() {
      return location;
    }
  }

}
