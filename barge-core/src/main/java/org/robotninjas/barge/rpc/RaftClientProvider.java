package org.robotninjas.barge.rpc;

import org.robotninjas.barge.Replica;

public interface RaftClientProvider {
    AsynchronousRaftClient get(Replica replica);
}
