package org.robotninjas.barge;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.util.concurrent.ExecutionException;

import static org.robotninjas.barge.rpc.ClientProto.CommitOperation;
import static org.robotninjas.barge.rpc.ClientProto.CommitOperationResponse;

public interface RaftService extends Service {

  ListenableFuture<CommitOperationResponse> commitOperationAsync(CommitOperation request);

  CommitOperationResponse commitOperation(CommitOperation request) throws ExecutionException, InterruptedException;

}
