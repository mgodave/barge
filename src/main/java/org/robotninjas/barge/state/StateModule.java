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

import com.google.inject.Exposed;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import javax.annotation.Nonnull;

public class StateModule extends PrivateModule {

  @Override
  protected void configure() {

    install(new FactoryModuleBuilder()
      .build(StateFactory.class));
    expose(StateFactory.class);

    install(new FactoryModuleBuilder()
      .build(ReplicaManagerFactory.class));

  }

  @Nonnull
  @Provides
  @Singleton
  @Exposed
  Context getContext(@Nonnull DefaultContext ctx) {
    ctx.init();
    return ctx;
  }

}
