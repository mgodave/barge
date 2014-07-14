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

import com.google.inject.PrivateModule;

import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.RaftCoreModule;
import org.robotninjas.barge.StateMachine;
import org.robotninjas.barge.jaxrs.client.HttpRaftClientProvider;
import org.robotninjas.barge.jaxrs.ws.WsEventListenersModule;
import org.robotninjas.barge.rpc.RaftClientProvider;
import org.robotninjas.barge.state.Raft;

import java.io.File;


/**
 */
public class JaxRsRaftModule extends PrivateModule {

  private final ClusterConfig clusterConfig;
  private final File logDir;
  private final StateMachine stateMachine;
  private final long timeoutInMs;

  public JaxRsRaftModule(ClusterConfig clusterConfig, File logDir, StateMachine stateMachine, long timeoutInMs) {
    this.clusterConfig = clusterConfig;
    this.logDir = logDir;
    this.stateMachine = stateMachine;
    this.timeoutInMs = timeoutInMs;
  }

  @Override
  protected void configure() {

    install(new WsEventListenersModule());

    install(RaftCoreModule.builder()
        .withTimeout(timeoutInMs)
        .withConfig(clusterConfig)
        .withLogDir(logDir)
        .withStateMachine(stateMachine)
        .build());

    bind(RaftClientProvider.class).to(HttpRaftClientProvider.class).asEagerSingleton();

    expose(RaftClientProvider.class);
    expose(Raft.class);
  }
}
