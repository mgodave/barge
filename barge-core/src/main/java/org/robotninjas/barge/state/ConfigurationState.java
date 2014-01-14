package org.robotninjas.barge.state;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.Entry;
import org.robotninjas.barge.proto.RaftEntry.Membership;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ConfigurationState {

  State state;

  long membershipIndex;
  Membership membership;

  final Replica local;
  ImmutableList<Replica> remote;

  final String localKey;

  public ConfigurationState(Replica self) {
    this.local = self;
    this.localKey = self.getKey();
    Membership.Builder membership = Membership.newBuilder();
    membership.addMembers(self.getKey());
    setMembership(0, membership.build());
  }
  
  public long getId() {
    return membershipIndex;
  }

  enum State {
    STABLE, TRANSITIONAL
  }

  @Nonnull
  public Replica self() {
    return local;
  }

  @Nonnull
  public List<Replica> remote() {
    return remote;
  }

  public boolean hasVote(Replica r) {
    String key = r.getKey();

    if (membership.getMembersList().contains(key)) {
      return true;
    }

    if (state == State.TRANSITIONAL) {
      if (membership.getProposedMembersList().contains(key)) {
        return true;
      }
    }

    return false;
  }

  public void add(long index, Entry entry) {
    checkArgument(entry.hasMembership());

    if (membership == null || index > membershipIndex) {
      setMembership(index, entry.getMembership());
    }
  }

  private void setMembership(long index, Membership membership) {
    boolean transitional = membership.getProposedMembersCount() != 0;

    Set<String> replicaKeys = Sets.newHashSet();
    replicaKeys.addAll(membership.getMembersList());
    if (transitional) {
      replicaKeys.addAll(membership.getProposedMembersList());
    }

    List<Replica> remotes = Lists.newArrayList();
    for (String replicaKey : replicaKeys) {
      Replica replica = Replica.fromString(replicaKey);
      if (replicaKey.equals(localKey)) {
        continue;
      }

      remotes.add(replica);
    }
    
    this.remote = ImmutableList.copyOf(remotes);
    this.membership = membership;
    this.membershipIndex = index;
    this.state = transitional ? State.TRANSITIONAL : State.STABLE;
  }

  public Membership getMembership() {
    return membership;
  }

  public boolean isTransitional() {
    return state == State.TRANSITIONAL;
  }
}
