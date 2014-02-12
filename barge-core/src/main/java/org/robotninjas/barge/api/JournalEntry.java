/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
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
package org.robotninjas.barge.api;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.Immutable;
import java.io.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 */
@Immutable
public final class
  JournalEntry implements Serializable {

  private final Object entry;

  public JournalEntry(Object entry) {
    this.entry = entry;
  }

  public boolean hasAppend() {
    return entry instanceof Append;
  }

  public Append getAppend() {
    return (Append) entry;
  }

  public static JournalEntry parseFrom(byte[] data) {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    try {
      return (JournalEntry) new ObjectInputStream(inputStream).readObject();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  public byte[] toByteArray() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
      objectOutputStream.writeObject(this);
      objectOutputStream.close();

      return outputStream.toByteArray();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JournalEntry))
      return false;

    JournalEntry that = (JournalEntry) o;

    return Objects.equal(entry, that.entry);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(entry);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("entry", entry)
      .toString();
  }

  public boolean hasCommit() {
    return entry instanceof Commit;
  }

  public Commit getCommit() {
    return (Commit) entry;
  }

  public boolean hasTerm() {
    return entry instanceof Term;
  }

  public Term getTerm() {
    return (Term) entry;
  }

  public boolean hasVote() {
    return entry instanceof Vote;
  }

  public Vote getVote() {
    return (Vote) entry;
  }

  public boolean hasMembership() {
    return false;
  }

  public Membership getMembership() {
    return (Membership) entry;
  }

  public boolean hasSnapshot() {
    return entry instanceof Snapshot;
  }

  public Snapshot getSnapshot() {
    return (Snapshot) entry;
  }

  public static class Builder {
    private Object entry;

    public JournalEntry build() {
      checkNotNull(entry);

      return new JournalEntry(entry);
    }

    public Builder setAppend(Append append) {
      entry = append;
      return this;
    }

    public Builder setTerm(Term term) {
      entry = term;
      return this;
    }

    public Builder setCommit(Commit commit) {
      entry = commit;
      return this;
    }

    public Builder setVote(Vote vote) {
      entry = vote;
      return this;
    }

    public Builder setSnapshot(Snapshot snapshot) {
      entry = snapshot;
      return this;
    }
  }
}
