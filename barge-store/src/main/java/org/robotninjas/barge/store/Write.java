package org.robotninjas.barge.store;

import static com.google.common.base.Objects.toStringHelper;

import java.io.Serializable;

import java.util.Arrays;
import java.util.Objects;
import static java.util.Objects.hash;


/**
 * A write operation to the underlying store.
 * <p>Writes a <tt>value</tt> for <tt>key</tt>.</p>
 */
public class Write implements Serializable {

  private final String key;
  private final byte[] value;

  public Write(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getKey() {
    return key;
  }

  @SuppressWarnings("UnusedDeclaration")
  public byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("key", key).add("value", value).toString();
  }

  @Override
  public int hashCode() {
    return hash(key, value);
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    final Write other = (Write) obj;

    return Objects.equals(this.key, other.key) && Arrays.equals(this.value, other.value);
  }
}
