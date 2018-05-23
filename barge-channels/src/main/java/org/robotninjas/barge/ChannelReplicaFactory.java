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

import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelReplicaFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelReplicaFactory.class);
    private static final AtomicLong counter = new AtomicLong(0);
    private final Map<String, Replica> replicas = Maps.newConcurrentMap();

    public Replica newReplica() {
        long c = counter.getAndIncrement();
        UUID id = UUID.nameUUIDFromBytes(Longs.toByteArray(c));
        LOGGER.debug("Generating UUID for node {} {}", c, id);
        ChannelReplica replica = new ChannelReplica(id);
        replicas.put(id.toString(), replica);
        return replica;
    }

    public ClusterConfig newClusterConfig(Replica local, Iterable<Replica> remotes) {
        return new ClusterConfig() {
            @Override
            public Replica local() {
                return local;
            }

            @Override
            public Iterable<Replica> remote() {
                return remotes;
            }

            @Override
            public Replica getReplica(String info) {
                if (!replicas.containsKey(info)) {
                    return replicas.put(info, newReplica());
                }
                return replicas.get(info);
            }
        };
    }
}
