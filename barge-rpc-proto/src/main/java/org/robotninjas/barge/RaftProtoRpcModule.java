package org.robotninjas.barge;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.PrivateModule;
import io.netty.channel.nio.NioEventLoopGroup;
import org.robotninjas.barge.rpc.RpcModule;

import java.io.File;

class RaftProtoRpcModule extends PrivateModule{

    private final ClusterConfig config;
    private final File logDir;
    private final StateMachine stateMachine;
    private Optional<NioEventLoopGroup> eventLoopGroup = Optional.absent();
    private long timeout;
    private ListeningExecutorService stateMachineExecutor;

    public RaftProtoRpcModule(ClusterConfig config, File logDir, StateMachine stateMachine) {
        this.config = config;
        this.logDir = logDir;
        this.stateMachine = stateMachine;
    }

    public void setNioEventLoop(NioEventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = Optional.of(eventLoopGroup);
    }


    @Override
    protected void configure() {
        Replica local = config.local();

        install(new RaftCoreModule(config,logDir,stateMachine));
        
        final NioEventLoopGroup eventLoop;
        if (eventLoopGroup.isPresent()) {
            eventLoop = eventLoopGroup.get();
        } else {
            eventLoop = new NioEventLoopGroup();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    eventLoop.shutdownGracefully();
                }
            });
        }
        install(new RpcModule(local.address(), eventLoop));


        bind(RaftService.class);
        expose(RaftService.class);


    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setStateMachineExecutor(ListeningExecutorService stateMachineExecutor) {
        this.stateMachineExecutor = stateMachineExecutor;
    }

    public ListeningExecutorService getStateMachineExecutor() {
        return stateMachineExecutor;
    }
}
