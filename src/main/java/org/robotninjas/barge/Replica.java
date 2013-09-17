package org.robotninjas.barge;

import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

public class Replica {

  private final InetSocketAddress address;

  Replica(InetSocketAddress address) {
    this.address = address;
  }

  public static Replica fromString(String info) {
    try {
      HostAndPort hostAndPort = HostAndPort.fromString(info);
      InetAddress addr = InetAddress.getByName(hostAndPort.getHostText());
      InetSocketAddress saddr = new InetSocketAddress(addr, hostAndPort.getPort());
      return new Replica(saddr);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return null;
  }

  public SocketAddress address() {
    return address;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(address());
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (o instanceof Replica) {
      Replica other = (Replica) o;
      return Objects.equal(address(), other.address());
    }

    return false;

  }

  @Override
  public String toString() {
    return address.getAddress().getHostName() + ":" + address.getPort();
  }
}
