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

package org.robotninjas.barge.log;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;

public class LogModule extends PrivateModule {

  private final File logDirectory;
  private final StateMachine stateMachine;
  private final Executor executor;

  public LogModule(@Nonnull File logDirectory, @Nonnull StateMachine stateMachine, @Nonnull Executor executor) {
    this.logDirectory = checkNotNull(logDirectory);
    this.stateMachine = checkNotNull(stateMachine);
    this.executor = checkNotNull(executor);
  }

  @Override
  protected void configure() {

    bind(StateMachine.class).toInstance(stateMachine);
    bind(StateMachineProxy.class);
    bind(Executor.class)
        .annotatedWith(StateExecutor.class)
        .toInstance(executor);
    bind(RaftLog.class).asEagerSingleton();
    expose(RaftLog.class);

  }

  @Nonnull
  @Provides
  @Singleton
  Journal getJournal() {

    try {

      final Journal journal = JournalBuilder.of(logDirectory).setPhysicalSync(true).open();

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          //noinspection EmptyCatchBlock
          try {
            journal.close();
          } catch (IOException e) {
            //TODO log it
          }
        }
      }));

      return journal;

    } catch (IOException e) {

      throw propagate(e);

    }

  }


}
