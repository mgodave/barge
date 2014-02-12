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
public class Append implements Serializable {

  private final long index;
  private final Entry entry;

  public Append(long index, Entry entry) {
    this.index = index;
    this.entry = entry;
  }

  public long getIndex() {
    return index;
  }

  public Entry getEntry() {
    return entry;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(entry) * 37 + (int) index;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Append))
      return false;

    Append that = (Append) obj;

    return index == that.index && Objects.equal(entry, that.entry);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("index", index)
      .add("entry", entry)
      .toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Entry entry;
    private long index;

    public Builder setEntry(Entry entry) {
      this.entry = entry;
      return this;
    }

    public Entry getEntry() {
      return entry;
    }

    public Builder setIndex(long index) {
      this.index = index;
      return this;
    }

    public Append build() {
      return new Append(index, entry);
    }
  }
}
