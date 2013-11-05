[![Build Status](https://travis-ci.org/mgodave/barge.png)](https://travis-ci.org/mgodave/barge)

barge (ALPHA)
=====

An implementation of the [Raft Concensus Protocol][1]. This supercedes my [previous attempt][2].

[1]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[2]: https://github.com/mgodave/raft

Todo
====
Barge is still a work in progress, there is a lot left to do to make this something that can be used in an actual project. Some of the major missing features are:

* Log Compaction (alpha2)
* Dynamic Membership (alpha2)
* Expanded Unit Test Coverage
* Integration Testing

Roadmap
=======
Barge is currently at 0.1.0-alpha1. I intend to release an alpha2 when the library is feature complete from the standpoint of the paper. Interest and involvement will
determine how the library progresses past the alpha stage.

Get It
======

```xml
<dependency>
    <groupId>org.robotninjas.barge</groupId>
    <artifactId>barge-core</artifactId>
    <version>0.1.0-alpha1</version>
</dependency>
```

Use It
======

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
