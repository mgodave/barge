package org.robotninjas.barge.rpc;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.RpcCallback;
import org.apache.commons.pool.ObjectPool;
import org.robotninjas.protobuf.netty.client.ClientController;
import org.robotninjas.protobuf.netty.client.NettyRpcChannel;
import org.robotninjas.barge.RaftException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static org.robotninjas.barge.rpc.RaftProto.*;

public class RaftClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);

  private final ObjectPool<NettyRpcChannel> channelPool;

  RaftClient(ObjectPool<NettyRpcChannel> channelPool) {
    this.channelPool = channelPool;
  }

  public ListenableFuture<RequestVoteResponse> requestVote(RequestVote request) {

    NettyRpcChannel channel = null;
    try {

      channel = channelPool.borrowObject();
      RaftService.Stub stub = RaftService.newStub(channel);
      ClientController controller = new ClientController(channel);
      RpcHandlerFuture<RequestVoteResponse> responseHandler =
        new RpcHandlerFuture<RequestVoteResponse>(controller);
      stub.requestVote(controller, request, responseHandler);
      channelPool.returnObject(channel);
      return responseHandler;

    } catch (Exception e) {

      LOGGER.debug("exception caught while calling requestVote", e);
      return immediateFailedFuture(e);

    } finally {

      try {
        if(null != channel) {
          channelPool.returnObject(channel);
        }
      } catch(Exception e) {
        // ignored
      }

    }

  }

  public ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntries request) {

    NettyRpcChannel channel = null;
    try {

      channel = channelPool.borrowObject();
      RaftService.Stub stub = RaftService.newStub(channel);
      ClientController controller = new ClientController(channel);
      RpcHandlerFuture<AppendEntriesResponse> responseHandler =
        new RpcHandlerFuture<AppendEntriesResponse>(controller);
      stub.appendEntries(controller, request, responseHandler);
      channelPool.returnObject(channel);
      return responseHandler;

    } catch (Exception e) {

      return immediateFailedFuture(e);

    } finally {

      try {
        if(null != channel) {
          channelPool.returnObject(channel);
        }
      } catch(Exception e) {
        // ignored
      }

    }

  }

  private static class RpcHandlerFuture<T> extends AbstractFuture<T> implements RpcCallback<T> {

    private final ClientController controller;

    private RpcHandlerFuture(ClientController controller) {
      this.controller = controller;
    }

    @Override
    public void run(T parameter) {

      if (null == parameter) {
        setException(new RaftException(controller.errorText()));
      } else {
        set(parameter);
      }

    }

  }

}
