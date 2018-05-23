/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.robotninjas.barge;

import static com.google.common.base.Functions.toStringFunction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Optional;
import javax.inject.Inject;
import org.robotninjas.barge.api.Append;
import org.robotninjas.barge.api.Commit;
import org.robotninjas.barge.api.Entry;
import org.robotninjas.barge.api.JournalEntry;
import org.robotninjas.barge.api.Snapshot;
import org.robotninjas.barge.api.Term;
import org.robotninjas.barge.api.Vote;
import org.robotninjas.barge.log.RaftJournal;

public class InMemoryJournal implements RaftJournal {
    private final ArrayList<JournalEntry> journal = new ArrayList<>();
    private final ClusterConfig config;

    @Inject
    public InMemoryJournal(ClusterConfig config) {
        this.config = config;
    }

    private JournalEntry read(int index) {
        return journal.get(index);
    }

    private int write(JournalEntry entry) {
        journal.add(entry);
        return journal.size() - 1;
    }

    @Override
    public synchronized Entry get(Mark mark) {
        ArrayIndexMark impl = (ArrayIndexMark) mark;
        JournalEntry entry = read(impl.getIndex());
        return entry.getAppend().getEntry();
    }

    @Override
    public synchronized void truncateTail(Mark mark) {
        ArrayIndexMark impl = (ArrayIndexMark) mark;
        int location = impl.getIndex();

        ListIterator<JournalEntry> itr = journal.listIterator(journal.size() - 1);
        while (itr.hasPrevious()) {
            if (itr.previousIndex() == location) {
                break;
            }
            itr.previous();
            itr.remove();
        }
    }

    @Override
    public synchronized Mark appendEntry(Entry entry, long index) {
        JournalEntry je =
            JournalEntry.newBuilder()
                .setAppend(Append.newBuilder()
                    .setEntry(entry)
                    .setIndex(index)
                    .build())
                .build();

        int location = write(je);
        return new ArrayIndexMark(location);
    }

    @Override
    public synchronized Mark appendTerm(long term) {
        int location =
            write(JournalEntry.newBuilder()
                .setTerm(Term.newBuilder()
                    .setTerm(term)
                    .build())
                .build());
        return new ArrayIndexMark(location);
    }

    @Override
    public synchronized Mark appendCommit(long commit) {
        int location =
            write(JournalEntry.newBuilder()
                .setCommit(Commit.newBuilder()
                    .setIndex(commit)
                    .build())
                .build());
        return new ArrayIndexMark(location);
    }

    @Override
    public synchronized Mark appendVote(Optional<Replica> vote) {
        int location =
            write(JournalEntry.newBuilder()
                .setVote(Vote.newBuilder()
                    .setVotedFor(vote.map(toStringFunction()).orElse(""))
                    .build())
                .build());
        return new ArrayIndexMark(location);
    }

    @Override
    public synchronized Mark appendSnapshot(File file, long index, long term) {
        int location =
            write(JournalEntry.newBuilder()
                .setSnapshot(Snapshot.newBuilder()
                    .setLastIncludedIndex(index)
                    .setLastIncludedTerm(term)
                    .setSnapshotFile(file.getName())
                    .build())
                .build());
        return new ArrayIndexMark(location);
    }

    @Override
    public synchronized void replay(Visitor visitor) {
        ListIterator<JournalEntry> itr = journal.listIterator();
        while (itr.hasNext()) {

            Mark mark = new ArrayIndexMark(itr.nextIndex());
            JournalEntry entry = itr.next();

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
    }

    @Override
    public synchronized void close() throws IOException {
        journal.clear();
    }

    static class ArrayIndexMark implements Mark {
        private final int index;

        public ArrayIndexMark(int index) {
            this.index = index;
        }

        int getIndex() {
            return index;
        }
    }
}
