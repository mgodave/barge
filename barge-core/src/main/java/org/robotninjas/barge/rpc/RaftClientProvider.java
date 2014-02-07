package org.robotninjas.barge.rpc;

import org.robotninjas.barge.Replica;

public interface RaftClientProvider {
    RaftClient get(Replica replica);
}
