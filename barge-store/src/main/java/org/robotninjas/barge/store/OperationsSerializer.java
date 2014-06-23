package org.robotninjas.barge.store;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import com.google.common.base.Throwables;

import java.io.*;

import java.nio.ByteBuffer;


/**
 */
public class OperationsSerializer {

  public byte[] serialize(Write write) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    try {
      ObjectOutputStream os = new ObjectOutputStream(out);
      os.writeObject(write);

      return out.toByteArray();
    } catch (IOException e) {
      throw new OperationsSerializationException(e);
    }
  }

  /**
   * @return a new Object mapper with configured deserializer for barge store writes and reads.
   */
  static ObjectMapper objectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule testModule = new SimpleModule("BargeStore", new Version(0, 1, 0, null, "org.robotninjas", "barge-store"))
        .addDeserializer(Write.class, new WriteDeserializer());
    mapper.registerModule(testModule);

    return mapper;
  }

  public static JacksonJaxbJsonProvider jacksonModule() {
    JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(objectMapper());

    return provider;
  }

  public Write deserialize(byte[] bytes) {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);

    try {
      return (Write) new ObjectInputStream(in).readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  public Write deserialize(ByteBuffer entry) {
    byte[] bytes = new byte[entry.remaining()];

    entry.get(bytes);

    return deserialize(bytes);
  }


  public static class OperationsSerializationException extends RuntimeException {

    public OperationsSerializationException(Throwable throwable) {
      super(throwable);
    }

  }

  public static class WriteDeserializer extends com.fasterxml.jackson.databind.JsonDeserializer<Write> {

    private static final byte[] EMPTY = new byte[0];

    @Override
    public Write deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
      byte[] value = EMPTY;
      String key = "";

      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        key = jsonParser.getCurrentName();

        jsonParser.nextToken();

        if (key.equals("value")) {
          value = jsonParser.getBinaryValue();
        }
      }

      jsonParser.close();

      return new Write(key, value);
    }
  }
}
