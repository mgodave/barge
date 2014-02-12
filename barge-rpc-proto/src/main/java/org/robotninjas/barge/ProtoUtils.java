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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.robotninjas.barge.api.*;
import org.robotninjas.barge.proto.RaftEntry;
import org.robotninjas.barge.proto.RaftProto;

import javax.annotation.Nullable;

/**
 */
public class ProtoUtils {

  public static AsyncFunction<? super RaftProto.AppendEntriesResponse, ? extends AppendEntriesResponse> convertAppendResponse = new AsyncFunction<RaftProto.AppendEntriesResponse, AppendEntriesResponse>() {
    @Override
    public ListenableFuture<AppendEntriesResponse> apply(RaftProto.AppendEntriesResponse input) throws Exception {
      return Futures.immediateFuture(ProtoUtils.convert(input));
    }
  };

  public static AsyncFunction<? super RaftProto.RequestVoteResponse, ? extends RequestVoteResponse> convertVoteResponse = new AsyncFunction<RaftProto.RequestVoteResponse, RequestVoteResponse>() {
    @Override
    public ListenableFuture<RequestVoteResponse> apply(RaftProto.RequestVoteResponse input) throws Exception {
      return Futures.immediateFuture(ProtoUtils.convert(input));
    }
  };

  private static Function<Entry, RaftEntry.Entry> convertEntry = new Function<Entry, RaftEntry.Entry>() {
    @Nullable
    @Override
    public RaftEntry.Entry apply(@Nullable Entry input) {
      return convert(input);
    }
  };
  private static Function<RaftEntry.Entry,Entry> convertEntryProto = new Function<RaftEntry.Entry, Entry>() {
    @Nullable
    @Override
    public Entry apply(@Nullable RaftEntry.Entry input) {
      return convert(input);
    }
  };

  public static Entry convert(RaftEntry.Entry input) {
    return new Entry(input.getCommand().toByteArray(), input.getTerm());
  }

  public static RaftEntry.Entry convert(Entry input) {
    return RaftEntry.Entry.newBuilder()
      .setTerm(input.getTerm())
      .setCommand(ByteString.copyFrom(input.getCommand()))
      .build();
  }

  public static RequestVote convert(RaftProto.RequestVote request) {
    return new RequestVote(
      request.getTerm(),
      request.getCandidateId(),
      request.getLastLogIndex(),
      request.getLastLogTerm());
  }

  public static RaftProto.RequestVoteResponse convert(RequestVoteResponse requestVoteResponse) {
    return RaftProto.RequestVoteResponse.newBuilder()
      .setTerm(requestVoteResponse.getTerm())
      .setVoteGranted(requestVoteResponse.getVoteGranted())
      .build();
  }

  public static RequestVoteResponse convert(RaftProto.RequestVoteResponse requestVoteResponse) {
    return new RequestVoteResponse(requestVoteResponse.getTerm(), requestVoteResponse.getVoteGranted());
  }

  public static AppendEntries convert(RaftProto.AppendEntries request) {
    return new AppendEntries(request.getTerm(),
      request.getLeaderId(),
      request.getPrevLogIndex(),
      request.getPrevLogTerm(),
      request.getCommitIndex(),
      Lists.transform(request.getEntriesList(), convertEntryProto));
  }


  public static RaftProto.AppendEntries convert(AppendEntries request) {
    return RaftProto.AppendEntries.newBuilder()
      .setTerm(request.getTerm())
      .setCommitIndex(request.getCommitIndex())
      .setPrevLogIndex(request.getPrevLogIndex())
      .setPrevLogTerm(request.getPrevLogTerm())
      .setLeaderId(request.getLeaderId())
      .addAllEntries(Lists.transform(request.getEntriesList(), convertEntry))
      .build();
  }

  public static RaftProto.AppendEntriesResponse convert(AppendEntriesResponse response) {
    return RaftProto.AppendEntriesResponse.newBuilder()
      .setTerm(response.getTerm())
      .setSuccess(response.getSuccess())
      .setLastLogIndex(response.getLastLogIndex())
      .build();
  }

  public static AppendEntriesResponse convert(RaftProto.AppendEntriesResponse response) {
    return new AppendEntriesResponse(
      response.getTerm(),
      response.getSuccess(),
      response.getLastLogIndex());
  }

  public static RaftProto.RequestVote convert(RequestVote request) {
    return RaftProto.RequestVote.newBuilder()
      .setTerm(request.getTerm())
      .setCandidateId(request.getCandidateId())
      .setLastLogIndex(request.getLastLogIndex())
      .setLastLogTerm(request.getLastLogTerm())
      .build();
  }
}