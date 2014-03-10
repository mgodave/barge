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

import javax.annotation.concurrent.Immutable;

/**
 */
@Immutable
public class RequestVote {
  private final String candidateId;
  private final long lastLogTerm;
  private final long lastLogIndex;
  private final long term;

  public RequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
    this.term = term;
    this.candidateId = candidateId;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
  }

  public String getCandidateId() {
    return candidateId;
  }

  public long getLastLogTerm() {
    return lastLogTerm;
  }

  public long getLastLogIndex() {
    return lastLogIndex;
  }

  public long getTerm() {
    return term;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RequestVote)) return false;

    RequestVote that = (RequestVote) o;

    return (lastLogIndex == that.lastLogIndex)
      && (lastLogTerm == that.lastLogTerm)
      && (term == that.term)
      && Objects.equal(candidateId, that.candidateId);
  }

  @Override
  public int hashCode() {
    int result = candidateId.hashCode();
    result = 31 * result + (int) (lastLogTerm ^ (lastLogTerm >>> 32));
    result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
    result = 31 * result + (int) (term ^ (term >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("candidateId", candidateId)
      .add("lastLogTerm", lastLogTerm)
      .add("lastLogIndex", lastLogIndex)
      .add("term", term)
      .toString();
  }

  public static RequestVote getDefaultInstance() {
    return new RequestVote(0, "", 0, 0);
  }

  public static class Builder {
    private long term;
    private String candidateId;
    private long lastLogIndex;
    private long lastLogTerm;

    public Builder setTerm(long term) {
      this.term = term;
      return this;
    }

    public Builder setCandidateId(String candidateId) {
      this.candidateId = candidateId;
      return this;
    }

    public Builder setLastLogIndex(long lastLogIndex) {
      this.lastLogIndex = lastLogIndex;
      return this;
    }

    public Builder setLastLogTerm(long lastLogTerm) {
      this.lastLogTerm = lastLogTerm;
      return this;
    }

    public RequestVote build() {
      return new RequestVote(term, candidateId, lastLogIndex, lastLogTerm);
    }
  }
}
