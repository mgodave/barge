/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.robotninjas.barge;

import java.util.Objects;
import java.util.UUID;
import org.jetlang.channels.MemoryRequestChannel;
import org.jetlang.channels.RequestChannel;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

public class ChannelReplica implements Replica, Comparable<ChannelReplica> {
    private final RequestChannel<AppendEntries, AppendEntriesResponse> appendEntriesChannel = new MemoryRequestChannel<>();
    private final RequestChannel<RequestVote, RequestVoteResponse> requestVoteChannel = new MemoryRequestChannel<>();
    private final UUID id;

    UUID getId() {
        return id;
    }

    RequestChannel<AppendEntries, AppendEntriesResponse> getAppendEntriesChannel() {
        return appendEntriesChannel;
    }

    RequestChannel<RequestVote, RequestVoteResponse> getRequestVoteChannel() {
        return requestVoteChannel;
    }

    ChannelReplica(UUID id) {
        this.id = id;
    }

    @Override
    public int compareTo(ChannelReplica o) {
        return id.compareTo(o.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChannelReplica that = (ChannelReplica) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appendEntriesChannel, requestVoteChannel, id);
    }

    @Override
    public String toString() {
        return id.toString();
    }
}
