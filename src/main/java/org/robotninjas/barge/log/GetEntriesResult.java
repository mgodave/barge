package org.robotninjas.barge.log;

import com.google.common.collect.ImmutableList;
import org.robotninjas.barge.rpc.RaftEntry;

public class GetEntriesResult {

  private final long prevEntryTerm;
  private final long prevEntryIndex;
  private final ImmutableList<RaftEntry.Entry> entries;

  public GetEntriesResult(long prevEntryTerm, long prevEntryIndex, ImmutableList<RaftEntry.Entry> entries) {
    this.prevEntryTerm = prevEntryTerm;
    this.prevEntryIndex = prevEntryIndex;
    this.entries = entries;
  }

  public long lastLogTerm() {
    return prevEntryTerm;
  }

  public long lastLogIndex() {
    return prevEntryIndex;
  }

  public ImmutableList<RaftEntry.Entry> entries() {
    return entries;
  }

}
