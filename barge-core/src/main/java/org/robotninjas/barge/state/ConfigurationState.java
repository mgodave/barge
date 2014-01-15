package org.robotninjas.barge.state;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.robotninjas.barge.RaftMembership;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.proto.RaftEntry.Entry;
import org.robotninjas.barge.proto.RaftEntry.Membership;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ConfigurationState {

  State state;

  long membershipIndex;
  Membership membership;

  final Replica self;
  // ImmutableList<Replica> remote;

  private ImmutableList<Replica> currentMembers;
  private ImmutableList<Replica> proposedMembers;

  private ImmutableList<Replica> allVotingMembers;

  long version;

  private RaftMembership clusterMembership;

  public ConfigurationState(Replica self) {
    this.self = self;
    
    // The starting configuration is _no_ configuration.
    // We must (externally) create an initial configuration log entry which will be replayed
    Membership.Builder membership = Membership.newBuilder();
//    membership.addMembers(self.getKey());
    setMembership(0, membership.build());
  }

  public long getId() {
    return membershipIndex;
  }

  public long getVersion() {
    return version;
  }

  enum State {
    STABLE, TRANSITIONAL
  }

  @Nonnull
  public Replica self() {
    return self;
  }

  // @Nonnull
  // public List<Replica> remote() {
  // return remote;
  // }

  public boolean hasVote(Replica r) {
    return allVotingMembers.contains(r);
    //
    // String key = r.getKey();
    //
    // if (membership.getMembersList().contains(key)) {
    // return true;
    // }
    //
    // if (state == State.TRANSITIONAL) {
    // if (membership.getProposedMembersList().contains(key)) {
    // return true;
    // }
    // }
    //
    // return false;
  }

  public void addMembershipEntry(long index, Entry entry) {
    checkArgument(entry.hasMembership());

    if (membership == null || index > membershipIndex) {
      setMembership(index, entry.getMembership());
    }
  }

  private synchronized void setMembership(long index, Membership membership) {
    boolean transitional = membership.getProposedMembersCount() != 0;

    List<Replica> proposedMembers = Lists.newArrayList();
    List<Replica> currentMembers = Lists.newArrayList();
    List<Replica> allVotingMembers = Lists.newArrayList();

    Set<String> allVotingMemberKeys = Sets.newHashSet();
    allVotingMemberKeys.addAll(membership.getMembersList());
    if (transitional) {
      allVotingMemberKeys.addAll(membership.getProposedMembersList());
    }

    // TODO: Reuse objects?
    Map<String, Replica> replicas = Maps.newHashMap();
    replicas.put(self.getKey(), self);

    for (String replicaKey : allVotingMemberKeys) {
      Replica replica = replicas.get(replicaKey);
      if (replica == null) {
        replica = Replica.fromString(replicaKey);
        replicas.put(replicaKey, replica);
      }
      allVotingMembers.add(replica);
    }

    for (String replicaKey : membership.getMembersList()) {
      Replica replica = replicas.get(replicaKey);
      if (replica == null) {
        throw new IllegalStateException();
      }
      currentMembers.add(replica);
    }

    for (String replicaKey : membership.getProposedMembersList()) {
      Replica replica = replicas.get(replicaKey);
      if (replica == null) {
        throw new IllegalStateException();
      }
      proposedMembers.add(replica);
    }

    // this.remote = ImmutableList.copyOf(remotes);
    this.membership = membership;
    this.membershipIndex = index;
    this.state = transitional ? State.TRANSITIONAL : State.STABLE;

    if (transitional) {
      this.clusterMembership = null;
      this.proposedMembers = ImmutableList.copyOf(proposedMembers);
    } else {
      this.clusterMembership = new RaftMembership(index, membership.getMembersList());
      this.proposedMembers = null;
    }

    this.currentMembers = ImmutableList.copyOf(currentMembers);
    this.allVotingMembers = ImmutableList.copyOf(allVotingMembers);

    this.version++;
  }

  public Membership getMembership() {
    return membership;
  }

  public boolean isTransitional() {
    return state == State.TRANSITIONAL;
  }

  public RaftMembership getClusterMembership() {
    return clusterMembership;
  }

  public List<Replica> getAllVotingMembers() {
    return allVotingMembers;
  }

  public List<Replica> getCurrentMembers() {
    return currentMembers;
  }

  public List<Replica> getProposedMembers() {
    return proposedMembers;
  }

  @Override
  public String toString() {
    return "ConfigurationState [state=" + state + ", currentMembers=" + currentMembers + ", proposedMembers="
        + proposedMembers + ", allVotingMembers=" + allVotingMembers + ", version=" + version + "]";
  }

  
}
