package org.robotninjas.barge.log;

import static com.google.common.base.Functions.toStringFunction;
import static journal.io.api.Journal.ReadType;
import static journal.io.api.Journal.WriteType;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import journal.io.api.Journal;
import journal.io.api.Location;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.Append;
import org.robotninjas.barge.api.Commit;
import org.robotninjas.barge.api.Entry;
import org.robotninjas.barge.api.JournalEntry;
import org.robotninjas.barge.api.Snapshot;
import org.robotninjas.barge.api.Term;
import org.robotninjas.barge.api.Vote;

class RaftJournal {

  private final Journal journal;
  private final ClusterConfig config;

  public RaftJournal(Journal journal, ClusterConfig config) {
    this.journal = journal;
    this.config = config;
  }

  private JournalEntry read(Location loc) {
    try {
      byte[] data = journal.read(loc, ReadType.SYNC);
      return JournalEntry.parseFrom(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void delete(Location loc) {
    try {
      journal.delete(loc);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Location write(JournalEntry entry) {
    try {
      return journal.write(entry.toByteArray(), WriteType.SYNC);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Entry get(Mark mark) {
    JournalEntry entry = read(mark.getLocation());
    return entry.getAppend().getEntry();
  }

  public void truncateHead(Mark mark) {
    try {
      for (Location loc : journal.undo(mark.getLocation())) {
        delete(loc);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void truncateTail(Mark mark) {
    try {

      Location location = mark.getLocation();
      Iterable<Location> locations = StreamSupport.stream(journal.redo(location).spliterator(), false).skip(1).collect(Collectors.toList());

      for (Location loc : locations) {
        delete(loc);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Mark appendEntry(Entry entry, long index) {
    JournalEntry je =
      JournalEntry.newBuilder()
        .setAppend(Append.newBuilder()
          .setEntry(entry)
          .setIndex(index)
          .build())
        .build();

    Location location = write(je);
    return new Mark(location);
  }

  public Mark appendTerm(long term) {
    Location location =
      write(JournalEntry.newBuilder()
        .setTerm(Term.newBuilder()
          .setTerm(term)
          .build())
        .build());
    return new Mark(location);
  }

  public Mark appendCommit(long commit) {
    Location location =
      write(JournalEntry.newBuilder()
        .setCommit(Commit.newBuilder()
          .setIndex(commit)
          .build())
        .build());
    return new Mark(location);
  }

  public Mark appendVote(Optional<Replica> vote) {
    Location location =
      write(JournalEntry.newBuilder()
        .setVote(Vote.newBuilder()
          .setVotedFor(vote.map(toStringFunction()::apply).orElse(""))
          .build())
        .build());
    return new Mark(location);
  }

  public Mark appendSnapshot(File file, long index, long term) {
    Location location =
      write(JournalEntry.newBuilder()
        .setSnapshot(Snapshot.newBuilder()
          .setLastIncludedIndex(index)
          .setLastIncludedTerm(term)
          .setSnapshotFile(file.getName())
          .build())
        .build());
    return new Mark(location);
  }

  public void replay(Visitor visitor) {

    try {
      for (Location location : journal.redo()) {

        JournalEntry entry = read(location);
        Mark mark = new Mark(location);

        if (entry.hasAppend()) {
          Append append = entry.getAppend();
          visitor.append(mark, append.getEntry(), append.getIndex());
        } else if (entry.hasCommit()) {
          Commit commit = entry.getCommit();
          visitor.commit(mark, commit.getIndex());
        } else if (entry.hasTerm()) {
          Term term = entry.getTerm();
          visitor.term(mark, term.getTerm());
        } else if (entry.hasVote()) {
          Vote vote = entry.getVote();
          String votedfor = vote.getVotedFor();
          Replica replica = votedfor == null
            ? null : config.getReplica(votedfor);
          visitor.vote(mark, Optional.ofNullable(replica));
        }

      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  interface Visitor {

    void term(Mark mark, long term);

    void vote(Mark mark, Optional<Replica> vote);

    void commit(Mark mark, long commit);

    void append(Mark mark, Entry entry, long index);

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
