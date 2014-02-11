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

import javax.annotation.concurrent.Immutable;

/**
 */
@Immutable
public class RequestVoteResponse {

  private final long term;
  private final boolean voteGranted;

  public RequestVoteResponse(long term, boolean voteGranted) {
    this.term = term;
    this.voteGranted = voteGranted;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public boolean getVoteGranted() {
    return voteGranted;
  }

  public long getTerm() {
    return term;
  }

  public static class Builder {
    private long term;
    private boolean voteGranted;

    public Builder setTerm(long term) {
      this.term = term;
      return this;
    }

    public Builder setVoteGranted(boolean voteGranted) {
      this.voteGranted = voteGranted;
      return this;
    }

    public RequestVoteResponse build() {
      return new RequestVoteResponse(term, voteGranted);
    }
  }
}
