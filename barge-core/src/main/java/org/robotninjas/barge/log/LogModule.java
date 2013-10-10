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

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.*;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import org.robotninjas.barge.StateMachine;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class LogModule extends PrivateModule {

  private final File logDirectory;
  private final StateMachine stateMachine;

  public LogModule(@Nonnull File logDirectory, @Nonnull StateMachine stateMachine) {
    this.logDirectory = checkNotNull(logDirectory);
    this.stateMachine = checkNotNull(stateMachine);
  }

  @Override
  protected void configure() {

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("State Machine Thread").build();
    final ListeningExecutorService stateMachineExecutor = listeningDecorator(newSingleThreadExecutor(threadFactory));

    bind(ListeningExecutorService.class)
      .annotatedWith(StateMachineExecutor.class)
      .toInstance(stateMachineExecutor);

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        stateMachineExecutor.shutdownNow();
      }
    }));

    bind(StateMachine.class).toInstance(stateMachine);
    bind(StateMachineProxy.class);

    bind(new TypeLiteral<Supplier<File>>() {}).toInstance(new Supplier<File>() {
      @Override
      public File get() {
        return new File("");
      }
    });

  }

  @Nonnull
  @Provides
  @Singleton
  @Exposed
  RaftLog getLog(@Nonnull DefaultRaftLog log) {
    log.init();
    return log;
  }

  @Nonnull
  @Provides
  @Singleton
  Journal getJournal() {

    try {

      /**
       * TODO Think more about what is really needed here.
       * This is really just the most basic configuration
       * possible. Specifically need to think through whether
       * we need a full sync to disk on every write. My suspicion
       * is yes. This configuration should probably be done by the
       * log.
       */
      final Journal journal = JournalBuilder.of(logDirectory).open();

      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          //noinspection EmptyCatchBlock
          try {
            journal.close();
          } catch (IOException e) {}
        }
      }));

      return journal;

    } catch (IOException e) {

      throw propagate(e);

    }

  }


}
