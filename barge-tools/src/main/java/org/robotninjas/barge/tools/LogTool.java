package org.robotninjas.barge.tools;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import org.robotninjas.barge.api.Append;
import org.robotninjas.barge.api.Commit;
import org.robotninjas.barge.api.JournalEntry;
import org.robotninjas.barge.api.Membership;
import org.robotninjas.barge.api.Snapshot;
import org.robotninjas.barge.api.Term;
import org.robotninjas.barge.api.Vote;

/**
 * For now this is just a simple program to print the contents of the log. I envision
 * this turning into a generic tool for working with the log.
 *
 */
public class LogTool {

  private enum Type {EMPTY, APPEND, COMMIT, MEMBERSHIP, SNAPSHOT, TERM, VOTE}

  public static void main(String... args) throws IOException {

    Journal journal = JournalBuilder.of(new File(args[0])).open();

    long term = 0;
    long index = 0;
    long committed = 0;
    List<String> membership = Collections.emptyList();
    Optional<String> lastVotedFor = Optional.empty();
    Type lastEntryType = Type.EMPTY;

    for (Location loc : journal.redo()) {

      byte[] rawEntry = journal.read(loc, Journal.ReadType.ASYNC);
      JournalEntry entry = JournalEntry.parseFrom(rawEntry);

      if (entry.hasAppend()) {

        Append a = entry.getAppend();
        index = a.getIndex();
        term = a.getEntry().getTerm();
        lastEntryType = Type.APPEND;

      } else if (entry.hasCommit()) {

        Commit c = entry.getCommit();
        committed = c.getIndex();
        lastEntryType = Type.COMMIT;

      } else if (entry.hasMembership()) {

        Membership m = entry.getMembership();
        membership = m.getMembersList();
        lastEntryType = Type.MEMBERSHIP;

      } else if (entry.hasSnapshot()) {

        Snapshot s = entry.getSnapshot();
        index = s.getLastIncludedIndex();
        term = s.getLastIncludedTerm();
        lastEntryType = Type.SNAPSHOT;

      } else if (entry.hasTerm()) {

        Term t = entry.getTerm();
        term = t.getTerm();
        lastEntryType = Type.TERM;

      } else if (entry.hasVote()) {

        Vote v = entry.getVote();
        lastVotedFor = Optional.ofNullable(v.getVotedFor());
        lastEntryType = Type.VOTE;

        System.out.println("Vote: " + lastVotedFor.orElse(null));

      }

      System.out.println(lastEntryType.toString() + " " + "term: " + term + ", index: " + index + ", committed: " + committed);
    }

    journal.close();

  }

}
