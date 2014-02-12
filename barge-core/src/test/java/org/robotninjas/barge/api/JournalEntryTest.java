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

import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 */
@RunWith(Theories.class)
public class JournalEntryTest {

  @DataPoints
  public static Object[] entries() {
    return new Object[]{
      new Commit(42),
      new Append(12, new Entry("foo".getBytes(), 2)),
      new Term(3),
      new Snapshot(12, 14, "bar"),
      new Vote("baz")
    };
  }

  @Theory
  @Test
  public void canWriteAndReadACommitEntry(Object data) throws Exception {
    JournalEntry entry = new JournalEntry(data);

    assertThat(JournalEntry.parseFrom(entry.toByteArray())).isEqualTo(entry);
  }
}
