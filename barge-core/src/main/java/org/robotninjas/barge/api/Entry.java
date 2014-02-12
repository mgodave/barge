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
import java.util.Arrays;

/**
 */
@Immutable
public class Entry  implements Serializable {

  private final byte[] command;
  private final long term;

  public Entry(byte[] command, long term) {
    this.command = Arrays.copyOf(command,command.length);
    this.term = term;
  }

  public long getTerm() {
    return term;
  }

  public byte[] getCommand() {
    return command;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(command) * 37 + (int) term;
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof Entry))
      return false;

    Entry that = (Entry)obj;
    return Arrays.equals(command,that.command) && term == that.term;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("command", command)
      .add("term", term)
      .toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private byte[] command;
    private long term;

    public Builder setCommand(byte[] command) {
      this.command = command;
      return this;
    }

    public Builder setTerm(long term) {
      this.term = term;
      return this;
    }

    public Entry build() {
      return new Entry(command,term)
;    }
  }
}
