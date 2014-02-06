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
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
@ThreadSafe
public class NettyReplica implements Replica {

  private final InetSocketAddress address;

  NettyReplica(@Nonnull InetSocketAddress address) {
    this.address = checkNotNull(address);
  }

  @Nonnull
  public static NettyReplica fromString(@Nonnull String info) {
    try {
      checkNotNull(info);
      HostAndPort hostAndPort = HostAndPort.fromString(info);
      InetAddress addr = InetAddress.getByName(hostAndPort.getHostText());
      InetSocketAddress saddr = new InetSocketAddress(addr, hostAndPort.getPort());
      return new NettyReplica(saddr);
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
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

    if (o instanceof NettyReplica) {
      NettyReplica other = (NettyReplica) o;
      return Objects.equal(address(), other.address());
    }

    return false;

  }

  @Nonnull
  @Override
  public String toString() {
    return address.getAddress().getHostName() + ":" + address.getPort();
  }
}
