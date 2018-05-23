/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.robotninjas.barge.log;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import org.robotninjas.barge.StateMachine;

public class LogModule extends PrivateModule {

    private final File logDirectory;
    private final StateMachine stateMachine;
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutorService;

    public LogModule(@Nonnull File logDirectory, @Nonnull StateMachine stateMachine, @Nonnull Executor executor, @Nonnull ScheduledExecutorService scheduledExecutorService) {
        this.logDirectory = checkNotNull(logDirectory);
        this.stateMachine = checkNotNull(stateMachine);
        this.executor = checkNotNull(executor);
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    protected void configure() {

        bind(StateMachine.class).toInstance(stateMachine);
        bind(StateMachineProxy.class);
        bind(Executor.class)
            .annotatedWith(StateExecutor.class)
            .toInstance(executor);
        bind(RaftLog.class).asEagerSingleton();
        bind(RaftJournal.class).to(RaftJournalImpl.class);
        expose(RaftLog.class);

    }

    @Nonnull
    @Provides
    @Singleton
    Journal getJournal() throws IOException {
        return JournalBuilder.of(logDirectory)
            .setDisposer(scheduledExecutorService)
            .setPhysicalSync(true)
            .open();

    }


}
