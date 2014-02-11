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

import com.google.protobuf.ByteString;

import javax.annotation.concurrent.Immutable;

/**
 */
@Immutable
public class Entry {
  private final ByteString command;
  private final long term;

  public Entry(ByteString command, long term) {
    this.command = command;
    this.term = term;
  }

  public long getTerm() {
    return term;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public ByteString getCommand() {
    return command;
  }

  public static class Builder {
    private ByteString command;
    private long term;

    public Builder setCommand(ByteString command) {
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
