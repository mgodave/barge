package org.robotninjas.barge.jaxrs.ws;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 */
@SuppressWarnings({"UnusedDeclaration", "getters make Jackson happier"})
public class WsMessage {
  
  private final String type;
  private final Date timestamp;

  public WsMessage(String type) {
    this.type = type;
    this.timestamp = new Date();
  }

  public String getType() {
    return type;
  }

  public String getTimestamp() {
    return SimpleDateFormat.getDateTimeInstance().format(timestamp);
  }
}
