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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;

import java.io.IOException;

/**
 * Utility methods for configuring Jackson.
 */
abstract class Jackson {


  /**
   *
   * @return a new Object mapper with configured deserializer for barge' object model.
   */
  static ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule testModule = new SimpleModule("MyModule", new Version(0, 1, 0, null, "org.robotninjas", "barge"))
      .addDeserializer(RequestVote.class, new RequestVoteDeserializer(RequestVote.class))
      .addDeserializer(RequestVoteResponse.class, new RequestVoteResponseDeserializer(RequestVoteResponse.class));
    mapper.registerModule(testModule);
    return mapper;
  }

  /**
   *
   * @return a suitable provider with configured deserialization objects.
   */
  static JacksonJaxbJsonProvider customJacksonProvider() {
    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(objectMapper());
    return provider;
  }

  static class RequestVoteDeserializer extends StdDeserializer<RequestVote> {

    protected RequestVoteDeserializer(Class<?> aClass) {
      super(aClass);
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

        if (fieldName.equals("term")) {
          builder.setTerm(jsonParser.getLongValue());
        } else if (fieldName.equals("candidateId")) {
          builder.setCandidateId(jsonParser.getText());
        } else if (fieldName.equals("lastLogIndex")) {
          builder.setLastLogIndex(jsonParser.getLongValue());
        } else if (fieldName.equals("lastLogTerm")) {
          builder.setLastLogTerm(jsonParser.getLongValue());
        }
      }
      jsonParser.close();
      return builder.build();
    }

  }

  public static class RequestVoteResponseDeserializer extends StdDeserializer<RequestVoteResponse> {

    public RequestVoteResponseDeserializer(Class<RequestVoteResponse> requestVoteResponseClass) {
      super(requestVoteResponseClass);
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
      jsonParser.close();
      return builder.build();

    }
  }
}
