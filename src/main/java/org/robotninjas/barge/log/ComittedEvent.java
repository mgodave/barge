package org.robotninjas.barge.log;

import java.nio.ByteBuffer;

public class ComittedEvent {

  private final ByteBuffer command;

  ComittedEvent(ByteBuffer command) {
    this.command = command;
  }

  public ByteBuffer command() {
    return command;
  }
}
