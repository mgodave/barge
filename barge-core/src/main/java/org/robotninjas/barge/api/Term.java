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
public class Term implements Serializable {

  private final long term;

  public Term(long term) {
    this.term = term;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long getTerm() {
    return term;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Term && term == ((Term) o).term;

  }

  @Override
  public int hashCode() {
    return (int) (term ^ (term >>> 32));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("term", term)
      .toString();
  }

  public static class Builder {
    private long term;

    public Builder setTerm(long term) {
      this.term = term;
      return this;
    }

    public Term build() {
      return new Term(term);
    }
  }
}
