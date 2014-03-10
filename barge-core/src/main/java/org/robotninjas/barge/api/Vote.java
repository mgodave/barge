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
import java.io.Serializable;

/**
 */
@Immutable
public class Vote implements Serializable {

  private final String votedFor;

  public Vote(String votedFor) {
    this.votedFor = votedFor;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public String getVotedFor() {
    return votedFor;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Vote && Objects.equal(votedFor, ((Vote) o).votedFor);

  }

  @Override
  public int hashCode() {
    return votedFor != null ? votedFor.hashCode() : 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("votedFor", votedFor)
      .toString();
  }

  public static class Builder {
    private String votedFor;

    public Builder setVotedFor(String votedFor) {
      this.votedFor = votedFor;
      return this;
    }

    public Vote build() {
      return new Vote(votedFor);
    }
  }
}
