package org.robotninjas.barge.jaxrs.ws;

import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.state.Raft;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

/**
 */
public class WsMessages {

  public static StateChangeMessage stateChange(Raft raft, Raft.StateType from, Raft.StateType to) {
    return new StateChangeMessage(raft.toString(), from, to);
  }

  public static WsMessage invalidTransition(Raft raft, Raft.StateType expected, Raft.StateType actual) {
    return new InvalidTransitionMessage(raft.toString(),expected,actual);
  }

  public static WsMessage stopping(Raft raft) {
    return new StoppingMessage(raft.toString());
  }

  public static WsMessage init(Raft raft) {
    return new InitMessage(raft.toString());
  }

  public static WsMessage appendEntries(Raft raft, AppendEntries entries) {
    return new AppendEntriesMessage(raft.toString(),entries);
  }

  public static WsMessage requestVote(Raft raft, RequestVote vote) {
    return new RequestVoteMessage(raft.toString(),vote);
  }

  public static WsMessage commit(Raft raft, byte[] operation) {
    return new CommitMessage(raft.toString(),operation);
  }

  @Immutable
  @SuppressWarnings({"UnusedDeclaration", "getters make Jackson happier"})
  private static class StateChangeMessage extends WsMessage {
    private final String target;
    private final Raft.StateType from;
    private final Raft.StateType to;

    public StateChangeMessage(String target, Raft.StateType from, Raft.StateType to) {
      super("stateChange");
      this.target = target;
      this.from = from;
      this.to = to;
    }

    public String getTarget() {
      return target;
    }

    public Raft.StateType getFrom() {
      return from;
    }

    public Raft.StateType getTo() {
      return to;
    }

    @Override
    public int hashCode() {
      return Objects.hash(target, from, to);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final StateChangeMessage other = (StateChangeMessage) obj;
      return Objects.equals(this.target, other.target) && Objects.equals(this.from, other.from) && Objects.equals(this.to, other.to);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("target", target)
          .add("from", from)
          .add("to", to)
          .toString();
    }
  }

  @SuppressWarnings({"UnusedDeclaration", "getters make Jackson happier"})
  @Immutable
  private static class InvalidTransitionMessage extends WsMessage {
    private final String target;
    private final Raft.StateType expected;
    private final Raft.StateType actual;

    public InvalidTransitionMessage(String target, Raft.StateType expected, Raft.StateType actual) {
      super("invalidTransition");
      this.target = target;
      this.expected = expected;
      this.actual = actual;
    }

    public String getTarget() {
      return target;
    }

    public Raft.StateType getExpected() {
      return expected;
    }

    public Raft.StateType getActual() {
      return actual;
    }

    @Override
    public int hashCode() {
      return Objects.hash(target, expected, actual);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final InvalidTransitionMessage other = (InvalidTransitionMessage) obj;
      return Objects.equals(this.target, other.target) && Objects.equals(this.expected, other.expected) && Objects.equals(this.actual, other.actual);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("target", target)
          .add("expected", expected)
          .add("actual", actual)
          .toString();
    }
  }

  @SuppressWarnings({"UnusedDeclaration", "getters make Jackson happier"})
  @Immutable
  private static class StoppingMessage extends WsMessage {
    private final String target;

    public StoppingMessage(String target) {
      super("stopping");
      this.target = target;
    }

    public String getTarget() {
      return target;
    }

    @Override
    public int hashCode() {
      return Objects.hash(target);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final StoppingMessage other = (StoppingMessage) obj;
      return Objects.equals(this.target, other.target);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("target", target)
          .toString();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  private static class InitMessage extends WsMessage {
    private final String name;

    public InitMessage(String name) {
      super("init");
      this.name = name;
    }

    public String getName() {
      return name;
    }

    @Override public int hashCode() {
      return Objects.hash(name);
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final InitMessage other = (InitMessage) obj;
      return Objects.equals(this.name, other.name);
    }

    @Override public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("name", name)
          .toString();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  private static class AppendEntriesMessage extends WsMessage {
    private final String name;
    private final AppendEntries entries;

    public AppendEntriesMessage(String name, AppendEntries entries) {
      super("appendEntries");
      this.name = name;
      this.entries = entries;
    }

    public String getName() {
      return name;
    }

    public AppendEntries getEntries() {
      return entries;
    }

    @Override public int hashCode() {
      return Objects.hash(name, entries);
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final AppendEntriesMessage other = (AppendEntriesMessage) obj;
      return Objects.equals(this.name, other.name) && Objects.equals(this.entries, other.entries);
    }

    @Override public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("name", name)
          .add("entries", entries)
          .toString();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  private static class RequestVoteMessage extends WsMessage {
    private final String name;
    private final RequestVote vote;

    public RequestVoteMessage(String name, RequestVote vote) {
      super("requestVote");
      this.name = name;
      this.vote = vote;
    }

    public String getName() {
      return name;
    }

    public RequestVote getVote() {
      return vote;
    }

    @Override public int hashCode() {
      return Objects.hash(name, vote);
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final RequestVoteMessage other = (RequestVoteMessage) obj;
      return Objects.equals(this.name, other.name) && Objects.equals(this.vote, other.vote);
    }

    @Override public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("name", name)
          .add("vote", vote)
          .toString();
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  private static class CommitMessage extends WsMessage {
    private final String name;
    private final byte[] operation;

    public CommitMessage(String name, byte[] operation) {
      super("commit");
      this.name = name;
      this.operation = operation;
    }

    public String getName() {
      return name;
    }

    public byte[] getOperation() {
      return operation;
    }

    @Override public int hashCode() {
      return Objects.hash(name, operation);
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final CommitMessage other = (CommitMessage) obj;
      return Objects.equals(this.name, other.name) && Objects.equals(this.operation, other.operation);
    }

    @Override public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("name", name)
          .add("operation", operation)
          .toString();
    }
  }
}
