package org.robotninjas.barge.log;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.nio.ByteBuffer;

@Immutable
public class ComittedEvent {

  private final ByteBuffer command;

  ComittedEvent(@Nonnull ByteBuffer command) {
    this.command = command;
  }

  @Nonnull
  public ByteBuffer command() {
    return command;
  }
}
