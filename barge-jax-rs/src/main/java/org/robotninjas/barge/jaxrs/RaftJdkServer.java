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

import com.google.common.base.Throwables;
import com.sun.net.httpserver.HttpServer;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.IOException;
import java.net.URI;


/**
 * A dedicated server for an instance of Raft using JDK's embedded HTTP server.
 */
public class RaftJdkServer implements RaftServer<RaftJdkServer> {

  private final int serverIndex;
  private final URI[] uris;
  private final RaftApplication application;

  private HttpServer httpServer;

  public RaftJdkServer(int serverIndex, URI[] uris, File logDir) {
    this.serverIndex = serverIndex;
    this.uris = uris;
    this.application = new RaftApplication(serverIndex, uris, logDir);
  }


  public RaftJdkServer start(int unusedPort) {
    this.httpServer = JdkHttpServerFactory.createHttpServer(UriBuilder.fromUri(uris[serverIndex]).path("raft").build(),
      application.makeResourceConfig());

    return this;
  }

  public void stop() {
    application.stop();
    httpServer.stop(0);
  }

  @Override
  public void clean() {

    try {
      application.clean();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }


}
