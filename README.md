barge
=====

An implementation of the [Raft Concensus Protocol][1]. This supercedes my [previous attempt][2]. In order to build, checkout the latest version of [netty-protobuf-rpc][3] and build with `mvn clean install`.

[1]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
[2]: https://github.com/mgodave/raft
[3]: https://github.com/mgodave/netty-protobuf-rpc

Notes
=====

* In order to catch a replica up, either because it crashed and restarted or it's just being slow, the leader will probe for the last entry in the follower with single entries then, once it has found the correct place, batch update the client to catch it up as soon as possible.
