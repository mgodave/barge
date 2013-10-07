package org.robotninjas.barge.log;

import journal.io.api.Location;

public class JournalIOLocation implements JournalLocation {

  private final Location location;

  public JournalIOLocation(Location location) {
    this.location = location;
  }

  public Location getLocation() {
    return location;
  }
}
