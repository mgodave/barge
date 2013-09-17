/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
