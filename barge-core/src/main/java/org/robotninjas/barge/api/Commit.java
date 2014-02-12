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
public class Commit  implements Serializable {

  private final long index;

  public Commit(long index) {
    this.index = index;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long getIndex() {
    return index;
  }

  @Override
  public int hashCode() {
    return (int) index;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Commit && index == ((Commit) obj).index;

  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("index", index)
      .toString();
  }

  public static class Builder {
    private long index;

    public Builder setIndex(long index) {
      this.index = index;
      return this;
    }

    public Commit build() {
      return new Commit(index);
    }
  }
}
