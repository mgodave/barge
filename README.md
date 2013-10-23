[![Build Status](https://travis-ci.org/mgodave/barge.png)](https://travis-ci.org/mgodave/barge)

barge
=====

An implementation of the [Raft Concensus Protocol][1]. This supercedes my [previous attempt][2]. In order to build, checkout the latest version of [netty-protobuf-rpc][3] and build with `mvn clean install`.

[1]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[2]: https://github.com/mgodave/raft
[3]: https://github.com/mgodave/netty-protobuf-rpc

Run a server state machine

```java

List<Replica> members = Lists.newArrayList(
  Replica.fromString("localhost:10001"),
  Replica.fromString("localhost:10002")
);

RaftService service = Raft.newDistributedStateMachine(local, members, logDir, new StateMachine() {

  long count = 0;

  @Override
  public void applyOperation(@Nonnull ByteBuffer entry) {
    try {
      CounterOp op = CounterOp.parseFrom(ByteString.copyFrom(entry));
      count += op.getAmount();
      System.out.println("count " + count);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
  }

});

service.startAsync().awaitRunning();

```