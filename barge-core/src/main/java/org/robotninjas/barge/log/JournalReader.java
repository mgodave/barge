package org.robotninjas.barge.log;

import journal.io.api.ClosedJournalException;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface JournalReader {

  ByteBuffer read(JournalLocation loc) throws IOException;

}
