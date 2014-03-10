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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

/**
 */
@Immutable
public class AppendEntries {

  private final long prevLogIndex;
  private final long prevLogTerm;
  private final List<Entry> entriesList;
  private final long term;
  private final String leaderId;
  private final long commitIndex;


  public AppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm, long commitIndex, List<Entry> entries) {
    this.term = term;
    this.leaderId = leaderId;
    this.prevLogIndex = prevLogIndex;
    this.prevLogTerm = prevLogTerm;
    this.commitIndex = commitIndex;
    this.entriesList = ImmutableList.copyOf(entries);
  }

  public long getPrevLogIndex() {
    return prevLogIndex;
  }

  public long getPrevLogTerm() {
    return prevLogTerm;
  }

  public List<Entry> getEntriesList() {
    return entriesList;
  }

  public long getTerm() {
    return term;
  }

  public String getLeaderId() {
    return leaderId;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long getEntriesCount() {
    return entriesList.size();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AppendEntries)) return false;

    AppendEntries that = (AppendEntries) o;

    return Objects.equal(entriesList,that.entriesList)
      && commitIndex == that.commitIndex
      && prevLogIndex == that.prevLogIndex
      && prevLogTerm == that.prevLogTerm
      && term == that.term
      && Objects.equal(leaderId, that.leaderId);
  }

  @Override
  public int hashCode() {
    int result = (int) (prevLogIndex ^ (prevLogIndex >>> 32));
    result = 31 * result + (int) (prevLogTerm ^ (prevLogTerm >>> 32));
    result = 31 * result + entriesList.hashCode();
    result = 31 * result + (int) (term ^ (term >>> 32));
    result = 31 * result + leaderId.hashCode();
    result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("prevLogIndex", prevLogIndex)
      .add("prevLogTerm", prevLogTerm)
      .add("entriesList", entriesList)
      .add("term", term)
      .add("leaderId", leaderId)
      .add("commitIndex", commitIndex)
      .toString();
  }

  public static AppendEntries getDefaultInstance() {
    return new AppendEntries(0, "", 0, 0, 0, Collections.<Entry>emptyList());
  }

  public static class Builder {
    private long term;
    private String leaderId;
    private long prevLogIndex;
    private long prevLogTerm;
    private long commitIndex;
    private List<Entry> entries = Lists.newArrayList();

    public Builder setTerm(long term) {
      this.term = term;
      return this;
    }

    public Builder setLeaderId(String leaderId) {
      this.leaderId = leaderId;
      return this;
    }

    public Builder setPrevLogIndex(long prevLogIndex) {
      this.prevLogIndex = prevLogIndex;
      return this;
    }

    public Builder setPrevLogTerm(long prevLogTerm) {
      this.prevLogTerm = prevLogTerm;
      return this;
    }

    public Builder setCommitIndex(long commitIndex) {
      this.commitIndex = commitIndex;
      return this;
    }

    public Builder addAllEntries(List<Entry> entries) {
      this.entries.addAll(entries);
      return this;
    }

    public AppendEntries build() {
      return new AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, commitIndex, entries);
    }

    public Builder addEntry(Entry entry) {
      this.entries.add(entry);
      return this;
    }
  }
}
