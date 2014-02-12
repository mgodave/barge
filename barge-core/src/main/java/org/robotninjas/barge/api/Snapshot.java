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
public class Snapshot implements Serializable {

  private final long lastIncludedIndex;
  private final long lastIncludedTerm;
  private final String snapshotFile;

  public Snapshot(long lastIncludedIndex, long lastIncludedTerm, String snapshotFile) {
    this.lastIncludedIndex = lastIncludedIndex;
    this.lastIncludedTerm = lastIncludedTerm;
    this.snapshotFile = snapshotFile;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public long getLastIncludedIndex() {
    return lastIncludedIndex;
  }

  public long getLastIncludedTerm() {
    return lastIncludedTerm;
  }

  @Override
  public int hashCode() {
    int result = (int) (lastIncludedIndex ^ (lastIncludedIndex >>> 32));
    result = 31 * result + (int) (lastIncludedTerm ^ (lastIncludedTerm >>> 32));
    result = 31 * result + (snapshotFile != null ? snapshotFile.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Snapshot))
      return false;

    Snapshot that = (Snapshot) obj;

    return lastIncludedIndex == that.lastIncludedIndex
      && lastIncludedTerm == that.lastIncludedTerm
      && Objects.equal(snapshotFile, that.snapshotFile);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("lastIncludedIndex", lastIncludedIndex)
      .add("lastIncludedTerm", lastIncludedTerm)
      .add("snapshotFile", snapshotFile)
      .toString();
  }

  public static class Builder {
    private long lastIncludedIndex;
    private long lastIncludedTerm;
    private String snapshotFile;

    public Builder setLastIncludedIndex(long lastIncludedIndex) {
      this.lastIncludedIndex = lastIncludedIndex;
      return this;
    }

    public Builder setLastIncludedTerm(long lastIncludedTerm) {
      this.lastIncludedTerm = lastIncludedTerm;
      return this;
    }

    public Builder setSnapshotFile(String snapshotFile) {
      this.snapshotFile = snapshotFile;
      return this;
    }

    public Snapshot build() {
      return new Snapshot(lastIncludedIndex, lastIncludedTerm, snapshotFile);
    }
  }
}
