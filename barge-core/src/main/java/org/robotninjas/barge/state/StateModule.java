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

package org.robotninjas.barge.state;

import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import java.util.Random;

public class StateModule extends PrivateModule {

  private final long electionTimeout;
  private final Random random;

  public StateModule(long electionTimeout, Random random) {
    this.electionTimeout = electionTimeout;
    this.random = random;
  }

  @Override
  protected void configure() {

    bind(Random.class).toInstance(random);

    bind(StateFactory.class).to(DefaultStateFactory.class);

    install(new FactoryModuleBuilder()
      .build(ReplicaManagerFactory.class));

    bind(Long.class)
      .annotatedWith(ElectionTimeout.class)
      .toInstance(electionTimeout);

    bind(Raft.class)
      .to(RaftStateContext.class)
      .asEagerSingleton();

    expose(Raft.class);

  }

}
