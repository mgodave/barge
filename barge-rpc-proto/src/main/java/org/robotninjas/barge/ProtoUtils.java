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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.proto.RaftProto;

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

  public static RequestVote convert(RaftProto.RequestVote request) {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public static RaftProto.RequestVoteResponse convert(RequestVoteResponse requestVoteResponse) {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public static RequestVoteResponse convert(RaftProto.RequestVoteResponse requestVoteResponse) {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public static AppendEntries convert(RaftProto.AppendEntries request) {
    return null;
  }

  public static RaftProto.AppendEntriesResponse convert(AppendEntriesResponse response) {
    return null;
  }

  public static AppendEntriesResponse convert(RaftProto.AppendEntriesResponse response) {
    return null;
  }

  public static RaftProto.AppendEntries convert(AppendEntries request) {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }

  public static RaftProto.RequestVote convert(RequestVote request) {
    return null;  //To change body of created methods use File | Settings | File Templates.
  }
}