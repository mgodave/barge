[![Build Status](https://travis-ci.org/mgodave/barge.png)](https://travis-ci.org/mgodave/barge)

barge
=====

An implementation of the [Raft Concensus Protocol][1]. This supercedes my [previous attempt][2]. In order to build, checkout the latest version of [netty-protobuf-rpc][3] and build with `mvn clean install`.

[1]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[2]: https://github.com/mgodave/raft
[3]: https://github.com/mgodave/netty-protobuf-rpc

Run a server state machine

```java

public class Test implements StateMachine {

  @Override
  public void applyOperation(@Nonnull ByteBuffer entry) {
    System.out.println(entry.getLong());
  }

  public static void main(String... args) throws Exception {

    final int port = Integer.parseInt(args[0]);

    Replica local = Replica.fromString("localhost:" + port);
    List<Replica> members = Lists.newArrayList(
      Replica.fromString("localhost:10000"),
      Replica.fromString("localhost:10001"),
      Replica.fromString("localhost:10002")
    );
    members.remove(local);

    File logDir = new File(args[0]);
    logDir.mkdir();

    RaftService raft = RaftService.newBuilder()
      .local(local)
      .members(members)
      .logDir(logDir)
      .timeout(300)
      .build(new Test());

    raft.startAsync().awaitRunning();

  }

}


```
