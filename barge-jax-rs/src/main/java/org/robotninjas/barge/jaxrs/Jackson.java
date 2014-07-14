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
package org.robotninjas.barge.jaxrs;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Iterables;
import org.robotninjas.barge.api.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Utility methods for configuring Jackson.
 */
public abstract class Jackson {


  /**
   * @return a new Object mapper with configured deserializer for barge' object model.
   */
  public static ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule raftMessagesModule = new SimpleModule("MyModule", new Version(0, 1, 0, null, "org.robotninjas", "barge"))
        .addDeserializer(RequestVote.class, new RequestVoteDeserializer())
        .addDeserializer(HttpClusterConfig.class, new HttpClusterConfigDeserializer())
        .addDeserializer(HttpReplica.class, new HttpReplicaDeserializer())
        .addDeserializer(RequestVoteResponse.class, new RequestVoteResponseDeserializer())
        .addDeserializer(AppendEntries.class, new AppendEntriesDeserializer())
        .addDeserializer(AppendEntriesResponse.class, new AppendEntriesResponseDeserializer());
    mapper.registerModule(raftMessagesModule);
    return mapper;
  }

  /**
   * @return a suitable provider with configured deserialization objects.
   */
  public static JacksonJaxbJsonProvider customJacksonProvider() {
    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(objectMapper());
    return provider;
  }

  private static class RequestVoteDeserializer extends StdDeserializer<RequestVote> {

    protected RequestVoteDeserializer() {
      super(RequestVote.class);
    }

    /**
     * @see <a href="http://www.cowtowncoder.com/blog/archives/2009/01/entry_132.html">Jackson Documentation</a>
     */
    @Override
    public RequestVote deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      RequestVote.Builder builder = RequestVote.newBuilder();

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        jsonParser.nextToken();

        switch (fieldName) {
          case "term":
            builder.setTerm(jsonParser.getLongValue());
            break;
          case "candidateId":
            builder.setCandidateId(jsonParser.getText());
            break;
          case "lastLogIndex":
            builder.setLastLogIndex(jsonParser.getLongValue());
            break;
          case "lastLogTerm":
            builder.setLastLogTerm(jsonParser.getLongValue());
            break;
        }
      }
      
      return builder.build();
    }

  }

  private static class RequestVoteResponseDeserializer extends StdDeserializer<RequestVoteResponse> {

    public RequestVoteResponseDeserializer() {
      super(RequestVoteResponse.class);
    }


    /**
     * @see <a href="http://www.cowtowncoder.com/blog/archives/2009/01/entry_132.html">Jackson Documentation</a>
     */
    @Override
    public RequestVoteResponse deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      RequestVoteResponse.Builder builder = RequestVoteResponse.newBuilder();

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        jsonParser.nextToken();

        if (fieldName.equals("term")) {
          builder.setTerm(jsonParser.getLongValue());
        } else if (fieldName.equals("voteGranted")) {
          builder.setVoteGranted(jsonParser.getBooleanValue());
        }
      }

      return builder.build();
    }
  }


  private static class AppendEntriesDeserializer extends StdDeserializer<AppendEntries> {
    public AppendEntriesDeserializer() {
      super(AppendEntries.class);
    }

    @Override
    public AppendEntries deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      AppendEntries.Builder builder = AppendEntries.newBuilder();

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        JsonToken token = jsonParser.nextToken();

        switch (fieldName) {
          case "term":
            builder.setTerm(jsonParser.getLongValue());
            break;
          case "prevLogIndex":
            builder.setPrevLogIndex(jsonParser.getLongValue());
            break;
          case "prevLogTerm":
            builder.setPrevLogTerm(jsonParser.getLongValue());
            break;
          case "commitIndex":
            builder.setCommitIndex(jsonParser.getLongValue());
            break;
          case "leaderId":
            builder.setLeaderId(jsonParser.getText());
            break;
          case "entriesList":
            if (token != JsonToken.START_ARRAY) {
              throw new IOException("entriesList should be an array, got " + token);
            }
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
              builder.addEntry(deserializeEntry(jsonParser));
            }
            break;
        }
      }

      return builder.build();
    }

    private Entry deserializeEntry(JsonParser jsonParser) throws IOException {
      Entry.Builder builder = Entry.newBuilder();


      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        jsonParser.nextToken();

        if (fieldName.equals("term")) {
          builder.setTerm(jsonParser.getLongValue());
        } else if (fieldName.equals("command")) {
          builder.setCommand(jsonParser.getBinaryValue());
        }
      }

      return builder.build();
    }
  }

  private static class AppendEntriesResponseDeserializer extends StdDeserializer<AppendEntriesResponse> {
    protected AppendEntriesResponseDeserializer() {
      super(AppendEntriesResponse.class);
    }

    @Override
    public AppendEntriesResponse deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        jsonParser.nextToken();

        switch (fieldName) {
          case "term":
            builder.setTerm(jsonParser.getLongValue());
            break;
          case "success":
            builder.setSuccess(jsonParser.getBooleanValue());
            break;
          case "lastLogIndex":
            builder.setLastLogIndex(jsonParser.getLongValue());
            break;
        }
      }

      return builder.build();
    }
  }

  private static class HttpClusterConfigDeserializer extends JsonDeserializer<HttpClusterConfig> {
    @Override
    public HttpClusterConfig deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      List<HttpReplica> replicas = Lists.newArrayList();

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        JsonToken token = jsonParser.nextToken();

        switch (fieldName) {
          case "cluster":
            if (token != JsonToken.START_ARRAY) {
              throw new IOException("cluster should be an array, got " + token);
            }
            while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
              replicas.add(jsonParser.readValueAs(HttpReplica.class));
            }
            break;
        }
      }

      return new HttpClusterConfig(replicas.get(0), Iterables.toArray(Iterables.skip(replicas, 1), HttpReplica.class));
    }
  }

  private static class HttpReplicaDeserializer extends JsonDeserializer<HttpReplica> {
    @Override
    public HttpReplica deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      URI uri = null;

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldName = jsonParser.getCurrentName();

        jsonParser.nextToken();

        switch (fieldName) {
          case "uri":
            String uriString = jsonParser.getValueAsString();
            try {
              uri = new URI(uriString);
            } catch (URISyntaxException e) {
              throw new IllegalStateException("failed to build proper URI from parsed uri field " + uriString);
            }
            break;
        }
      }

      return new HttpReplica(uri);

    }
  }
}
