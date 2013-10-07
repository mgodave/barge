package org.robotninjas.barge.log;

import journal.io.api.Journal;
import journal.io.api.Location;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JournalIOWriter implements JournalWriter {

  private final Journal journal;

  public JournalIOWriter(Journal journal) {
    this.journal = journal;
  }

  @Override
  public JournalLocation write(ByteBuffer entry) throws IOException {
    byte[] data = new byte[entry.remaining()];
    entry.get(data);
    Location location = journal.write(data, Journal.WriteType.SYNC);
    return new JournalIOLocation(location);
  }

}
