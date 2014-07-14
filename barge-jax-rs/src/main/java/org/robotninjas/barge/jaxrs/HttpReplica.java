/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
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
package org.robotninjas.barge.jaxrs;

import com.google.common.base.Objects;
import org.robotninjas.barge.Replica;

import java.net.URI;

/**
 */
public class HttpReplica implements Replica {

  private final URI uri;

  public HttpReplica(URI uri) {
    this.uri = uri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HttpReplica)) return false;

    HttpReplica that = (HttpReplica) o;

    return Objects.equal(uri, that.uri);
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }

  @Override
  public String toString() {
    return uri.toString();
  }

  boolean match(URI uri) {
    return this.uri.equals(uri);
  }

  public URI getUri() {
    return uri;
  }
}
