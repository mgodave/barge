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
public class AppendEntriesResponse {

  private final long term;
  private final boolean success;
  private final long lastLogIndex;

  public AppendEntriesResponse(long term, boolean success, long lastLogIndex) {
    this.term = term;
    this.success = success;
    this.lastLogIndex = lastLogIndex;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public boolean getSuccess() {
    return success;
  }

  public long getTerm() {
    return term;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AppendEntriesResponse)) return false;

    AppendEntriesResponse that = (AppendEntriesResponse) o;

    return lastLogIndex == that.lastLogIndex
      && success == that.success
      && term == that.term;
  }

  @Override
  public int hashCode() {
    int result = (int) (term ^ (term >>> 32));
    result = 31 * result + (success ? 1 : 0);
    result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("term", term)
      .add("success", success)
      .add("lastLogIndex", lastLogIndex)
      .toString();
  }

  public long getLastLogIndex() {
    return lastLogIndex;
  }

  public static class Builder {
    private long term;
    private boolean success;
    private long lastLogIndex;

    public Builder setTerm(long term) {
      this.term = term;
      return this;
    }

    public Builder setSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Builder setLastLogIndex(long lastLogIndex) {
      this.lastLogIndex = lastLogIndex;
      return this;
    }

    public AppendEntriesResponse build() {
      return new AppendEntriesResponse(term,success,lastLogIndex);
    }
  }
}
