/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.robotninjas.barge;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Test;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.Entry;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.proto.RaftEntry;
import org.robotninjas.barge.proto.RaftProto;

/**
 */
public class ProtoUtilsTest {


  @Test
  public void canConvertRequestVoteMessageToAndFromProtobuf() throws Exception {
    RequestVote pojo = new RequestVote(12, "foo", 43, 44);
    RaftProto.RequestVote proto = RaftProto.RequestVote.newBuilder()
      .setTerm(12)
      .setCandidateId("foo")
      .setLastLogIndex(43)
      .setLastLogTerm(44)
      .build();

    assertThat(ProtoUtils.convert(pojo)).isEqualTo(proto);
    assertThat(ProtoUtils.convert(proto)).isEqualTo(pojo);
  }

  @Test
  public void canConvertRequestVoteResponseMessageToAndFromProtobuf() throws Exception {
    RequestVoteResponse pojo = new RequestVoteResponse(12, true);
    RaftProto.RequestVoteResponse proto = RaftProto.RequestVoteResponse.newBuilder()
      .setTerm(12)
      .setVoteGranted(true)
      .build();

    assertThat(ProtoUtils.convert(pojo)).isEqualTo(proto);
    assertThat(ProtoUtils.convert(proto)).isEqualTo(pojo);
  }

  @Test
  public void canConvertAppendEntriesToAndFromProtobuf() throws Exception {
    List<Entry> entries = Lists.newArrayList(new Entry("bar".getBytes(), 12), new Entry("baz".getBytes(), 12));
    AppendEntries pojo = new AppendEntries(12, "foo", 13, 14, 15, entries);
    RaftProto.AppendEntries proto = RaftProto.AppendEntries.newBuilder()
      .setTerm(12)
      .setLeaderId("foo")
      .setPrevLogIndex(13)
      .setPrevLogTerm(14)
      .setCommitIndex(15)
      .addEntries(RaftEntry.Entry.newBuilder()
        .setTerm(12)
        .setCommand(ByteString.copyFromUtf8("bar"))
        .build())
      .addEntries(RaftEntry.Entry.newBuilder()
        .setTerm(12)
        .setCommand(ByteString.copyFromUtf8("baz"))
        .build())
      .build();

    assertThat(ProtoUtils.convert(pojo)).isEqualTo(proto);
    assertThat(ProtoUtils.convert(proto)).isEqualTo(pojo);
  }

  @Test
  public void canConvertAppendEntriesResponseToAndFromProtobuf() throws Exception {
    AppendEntriesResponse pojo = new AppendEntriesResponse(12,true,43);
    RaftProto.AppendEntriesResponse proto = RaftProto.AppendEntriesResponse.newBuilder()
      .setTerm(12)
      .setLastLogIndex(43)
      .setSuccess(true)
      .build();

    assertThat(ProtoUtils.convert(pojo)).isEqualTo(proto);
    assertThat(ProtoUtils.convert(proto)).isEqualTo(pojo);
  }
}
