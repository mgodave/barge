package org.robotninjas.barge.log;

import journal.io.api.ClosedJournalException;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface JournalWriter {

  JournalLocation write(ByteBuffer entry) throws IOException;

}
