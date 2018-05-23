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
package org.robotninjas.barge.log;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.Entry;

public interface RaftJournal extends Closeable {
    Entry get(Mark mark);

    void truncateTail(Mark mark);

    Mark appendEntry(Entry entry, long index);

    Mark appendTerm(long term);

    Mark appendCommit(long commit);

    Mark appendVote(Optional<Replica> vote);

    Mark appendSnapshot(File file, long index, long term);

    void replay(Visitor visitor);

    @Override
    void close() throws IOException;

    interface Visitor {

        void term(Mark mark, long term);

        void vote(Mark mark, Optional<Replica> vote);

        void commit(Mark mark, long commit);

        void append(Mark mark, Entry entry, long index);

    }

    interface Mark {
    }

}
