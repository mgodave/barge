package org.robotninjas.barge.log;

import static com.google.common.base.Functions.toStringFunction;
import static java.util.stream.Collectors.toList;
import static journal.io.api.Journal.ReadType;
import static journal.io.api.Journal.WriteType;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
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

class RaftJournalImpl implements RaftJournal {

  private final Journal journal;
  private final ClusterConfig config;

  @Inject
  public RaftJournalImpl(Journal journal, ClusterConfig config) {
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

  @Override
  public Entry get(Mark mark) {
    MarkImpl impl = (MarkImpl) mark;
    JournalEntry entry = read(impl.getLocation());
    return entry.getAppend().getEntry();
  }

  @Override
  public void truncateTail(Mark mark) {
    try {
      MarkImpl impl = (MarkImpl) mark;
      Location location = impl.getLocation();
      Iterable<Location> locations = StreamSupport.stream(journal.redo(location).spliterator(), false).skip(1).collect(toList());

      for (Location loc : locations) {
        delete(loc);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Mark appendEntry(Entry entry, long index) {
    JournalEntry je =
      JournalEntry.newBuilder()
        .setAppend(Append.newBuilder()
          .setEntry(entry)
          .setIndex(index)
          .build())
        .build();

    Location location = write(je);
    return new MarkImpl(location);
  }

  @Override
  public Mark appendTerm(long term) {
    Location location =
      write(JournalEntry.newBuilder()
        .setTerm(Term.newBuilder()
          .setTerm(term)
          .build())
        .build());
    return new MarkImpl(location);
  }

  @Override
  public Mark appendCommit(long commit) {
    Location location =
      write(JournalEntry.newBuilder()
        .setCommit(Commit.newBuilder()
          .setIndex(commit)
          .build())
        .build());
    return new MarkImpl(location);
  }

  @Override
  public Mark appendVote(Optional<Replica> vote) {
    Location location =
      write(JournalEntry.newBuilder()
        .setVote(Vote.newBuilder()
          .setVotedFor(vote.map(toStringFunction()).orElse(""))
          .build())
        .build());
    return new MarkImpl(location);
  }

  @Override
  public Mark appendSnapshot(File file, long index, long term) {
    Location location =
      write(JournalEntry.newBuilder()
        .setSnapshot(Snapshot.newBuilder()
          .setLastIncludedIndex(index)
          .setLastIncludedTerm(term)
          .setSnapshotFile(file.getName())
          .build())
        .build());
    return new MarkImpl(location);
  }

  @Override
  public void replay(Visitor visitor) {

    try {
      for (Location location : journal.redo()) {

        JournalEntry entry = read(location);
        Mark mark = new MarkImpl(location);

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

  @Override
  public void close() throws IOException {
    journal.close();
  }

  static class MarkImpl implements Mark {

    private final Location location;

    MarkImpl(Location location) {
      this.location = location;
    }

    Location getLocation() {
      return location;
    }
  }

}
