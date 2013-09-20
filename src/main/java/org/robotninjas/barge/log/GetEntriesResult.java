/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
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

package org.robotninjas.barge.log;

import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.List;

import static org.robotninjas.barge.proto.RaftEntry.Entry;

@Immutable
@ThreadSafe
public class GetEntriesResult {

  private final long prevEntryTerm;
  private final long prevEntryIndex;
  private final List<Entry> entries;

  public GetEntriesResult(long prevEntryTerm, long prevEntryIndex, List<Entry> entries) {
    this.prevEntryTerm = prevEntryTerm;
    this.prevEntryIndex = prevEntryIndex;
    this.entries = Lists.newArrayList(entries);
  }

  public long lastLogTerm() {
    return prevEntryTerm;
  }

  public long lastLogIndex() {
    return prevEntryIndex;
  }

  @Nonnull
  public List<Entry> entries() {
    return Collections.unmodifiableList(entries);
  }

}
