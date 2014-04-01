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

import com.google.inject.Guice;
import com.google.inject.Injector;

import com.sun.net.httpserver.HttpServer;

import org.assertj.core.util.Files;

import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.Binder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.state.Raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.net.URI;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;


/**
 * A dedicated server for an instance of Raft.
 */
public class RaftHttpServer {

  private static final Logger logger = LoggerFactory.getLogger(RaftHttpServer.class);

  private final int serverIndex;
  private final URI[] uris;

  private HttpServer httpServer;

  public RaftHttpServer(int serverIndex, URI[] uris) {
    this.serverIndex = serverIndex;
    this.uris = uris;
  }

  public void stop(int timeout) {
    httpServer.stop(timeout);
  }

  public RaftHttpServer start() throws IOException {

    ClusterConfig clusterConfig = HttpClusterConfig.from(new HttpReplica(uris[serverIndex]),
        new HttpReplica(uris[(serverIndex + 1) % 3]), new HttpReplica(uris[(serverIndex + 2) % 3]));

    File logDir = new File("log" + serverIndex);

    if (logDir.isDirectory())
      Files.delete(logDir);

    if (!logDir.mkdirs())
      logger.warn("failed to create directories for storing logs, bad things may happen");

    StateMachine stateMachine = new StateMachine() {
        int i = 0;

        @Override public Object applyOperation(@Nonnull ByteBuffer entry) {
          return i++;
        }
      };

    final JaxRsRaftModule raftModule = new JaxRsRaftModule(clusterConfig, logDir, stateMachine, 1500);

    final Injector injector = Guice.createInjector(raftModule);

    ResourceConfig resourceConfig = new ResourceConfig();

    Binder binder = new AbstractBinder() {
        @Override protected void configure() {
          bindFactory(new Factory<Raft>() {
              @Override public Raft provide() {
                return injector.getInstance(Raft.class);
              }

              @Override public void dispose(Raft raft) {
              }
            }).to(Raft.class);
        }
      };

    resourceConfig.register(BargeResource.class);
    resourceConfig.register(Jackson.customJacksonProvider());
    resourceConfig.register(binder);

    this.httpServer = JdkHttpServerFactory.createHttpServer(uris[serverIndex], resourceConfig);

    return this;
  }
}
