package org.robotninjas.barge.log;

import com.google.common.base.Preconditions;
import journal.io.api.Journal;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JournalIOReader implements JournalReader {

  private final Journal journal;

  public JournalIOReader(Journal journal) {
    this.journal = journal;
  }

  @Override
  public ByteBuffer read(JournalLocation loc) throws IOException {
    Preconditions.checkArgument(loc instanceof JournalIOLocation);
    JournalIOLocation jioloc = (JournalIOLocation) loc;
    byte[] data = journal.read(jioloc.getLocation(), Journal.ReadType.SYNC);
    return ByteBuffer.wrap(data).asReadOnlyBuffer();
  }
}
