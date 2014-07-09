package org.robotninjas.barge.jaxrs.ws;

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

  @Immutable
  @SuppressWarnings({"UnusedDeclaration", "getters make Jackson happier"})
  private static class StateChangeMessage extends WsMessage {
    private final String target;
    private final Raft.StateType from;
    private final Raft.StateType to;

    public StateChangeMessage(String target, Raft.StateType from, Raft.StateType to) {
      super("state.change");
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
      super("invalid.transition");
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
}
