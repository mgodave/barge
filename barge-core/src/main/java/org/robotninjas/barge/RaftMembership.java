package org.robotninjas.barge;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RaftMembership {

  final long id;
  final ImmutableList<String> members;

  public RaftMembership(long id, Collection<String> members) {
    this.id = id;
    List<String> sorted = Lists.newArrayList(members);
    Collections.sort(sorted);
    this.members = ImmutableList.copyOf(sorted);
  }

  public long getId() {
    return id;
  }

  public ImmutableList<String> getMembers() {
    return members;
  }

  public RaftMembership merge(RaftMembership other) {
    Set<String> merged = Sets.newHashSet();
    merged.addAll(members);
    merged.addAll(other.members);

    return new RaftMembership(-1L, merged);
  }

}
