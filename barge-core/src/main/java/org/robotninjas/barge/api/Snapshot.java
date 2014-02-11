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
public class Snapshot  {

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
      return new Snapshot(lastIncludedIndex,lastIncludedTerm,snapshotFile);
    }
  }
}
