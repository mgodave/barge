package org.robotninjas.barge.tools;

import com.google.common.base.Optional;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import org.robotninjas.barge.proto.LogProto;
import org.robotninjas.barge.proto.RaftEntry.Entry;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * For now this is just a simple program to print the contents of the log. I envision
 * this turning into a generic tool for working with the log.
 *
 */
public class LogTool {

  private static enum Type {EMPTY, APPEND, COMMIT, MEMBERSHIP, SNAPSHOT, TERM, VOTE}

  public static void main(String... args) throws IOException {

    Journal journal = JournalBuilder.of(new File(args[0])).open();

    long term = 0;
    long index = 0;
    long committed = 0;
    List<String> membership = Collections.emptyList();
    Optional<String> lastVotedFor = Optional.absent();
    Type lastEntryType = Type.EMPTY;

    for (Location loc : journal.redo()) {

      byte[] rawEntry = journal.read(loc, Journal.ReadType.ASYNC);
      LogProto.JournalEntry entry = LogProto.JournalEntry.parseFrom(rawEntry);

      if (entry.hasAppend()) {

        LogProto.Append a = entry.getAppend();
        index = a.getIndex();

        Entry appendEntry = a.getEntry();
        if (appendEntry.hasMembership()) {
            lastEntryType = Type.MEMBERSHIP;
        }
        if (appendEntry.hasCommand()) {
            lastEntryType = Type.APPEND;
        }
        term = a.getEntry().getTerm();

      } else if (entry.hasCommit()) {

        LogProto.Commit c = entry.getCommit();
        committed = c.getIndex();
        lastEntryType = Type.COMMIT;

      } else if (entry.hasSnapshot()) {

        LogProto.Snapshot s = entry.getSnapshot();
        index = s.getLastIncludedIndex();
        term = s.getLastIncludedTerm();
        lastEntryType = Type.SNAPSHOT;

      } else if (entry.hasTerm()) {

        LogProto.Term t = entry.getTerm();
        term = t.getTerm();
        lastEntryType = Type.TERM;

      } else if (entry.hasVote()) {

        LogProto.Vote v = entry.getVote();
        lastVotedFor = Optional.fromNullable(v.getVotedFor());
        lastEntryType = Type.VOTE;

        System.out.println("Vote: " + lastVotedFor.orNull());

      }

      System.out.println(lastEntryType.toString() + " " + "term: " + term + ", index: " + index + ", committed: " + committed);
    }

    journal.close();

  }

}
