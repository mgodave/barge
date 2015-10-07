[![Build Status](https://travis-ci.org/mgodave/barge.png)](https://travis-ci.org/mgodave/barge)

barge (ALPHA)
=====

An implementation of the [Raft Consensus Protocol][1].

[1]: http://raftconsensus.github.io/
[2]: irc://chat.freenode.net/barge

Todo
====
Barge is still a work in progress, two major missing features are:

* Log Compaction (alpha2)
* Dynamic Membership (alpha2)

Roadmap
=======
Barge is currently at 0.1.0-alpha1. I intend to release an alpha2 when the library is feature complete from the standpoint of the paper. Interest and involvement will determine how the library progresses past the alpha stage.

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

    ClusterConfig config = ClusterConfig.from(
      Replica.fromString("localhost:10000"), // local
      Replica.fromString("localhost:10001"), // remote
      Replica.fromString("localhost:10002")  // remote
    );

    File logDir = new File(args[0]);
    logDir.mkdir();

    // configure the service
    RaftService raft = 
      RaftService.newBuilder(config)
        .logDir(logDir)
        .timeout(300)
        .build(new Test());

    // start this replica
    raft.startAsync().awaitRunning();
    
    // let's commit some things
    for (int i = 0; i < 10; i++) {
      raft.commit(new byte[] {'O', '_', 'o'}).get();
    }

  }

}


```



