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
package org.robotninjas.barge.jaxrs;

import org.robotninjas.barge.api.*;

/**
 */
public class Model {
  
  public static final RequestVote vote = RequestVote.newBuilder()
    .setCandidateId("id")
    .setLastLogIndex(12)
    .setLastLogTerm(13)
    .setTerm(13)
    .build();
  
  public static final RequestVoteResponse voteResponse = RequestVoteResponse.newBuilder()
    .setVoteGranted(true)
    .setTerm(13)
    .build();
  
  public static final AppendEntries entries = AppendEntries.newBuilder()
    .setLeaderId("foo")
    .addEntry(Entry.newBuilder()
      .setCommand("command".getBytes())
      .setTerm(2).build())
    .addEntry(Entry.newBuilder()
      .setCommand("command1".getBytes())
      .setTerm(2).build())
    .setCommitIndex(1)
    .setPrevLogIndex(2)
    .setPrevLogTerm(3)
    .setTerm(3)
    .build();
  
  public static final AppendEntriesResponse entriesResponse = AppendEntriesResponse.newBuilder()
    .setLastLogIndex(12)
    .setSuccess(true)
    .setTerm(3)
    .build();
}
