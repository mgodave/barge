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

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;

public class Raft {

  private static final Logger LOGGER = LoggerFactory.getLogger(Raft.class);
  private static long TIMEOUT = 300;

  public static void main(String... args) throws UnknownHostException {

    int port = Integer.parseInt(args[0]);

    Replica local = Replica.fromString("localhost:" + port);
    List<Replica> members = Lists.newArrayList(
      Replica.fromString("localhost:10000"),
      Replica.fromString("localhost:10001"),
      Replica.fromString("localhost:10002")
    );
    members.remove(local);

    File logDir = Files.createTempDir();
    LOGGER.info("Log dir: {}", logDir);
    Injector injector = Guice.createInjector(new RaftModule(local, members, TIMEOUT, logDir));
    RaftService service = injector.getInstance(RaftService.class);
    service.addLogListener(new LogListener() {
      @Override
      public void applyOperation(@Nonnull ByteBuffer entry) {
        System.out.println(entry.getLong());
      }
    });
    service.startAsync().awaitRunning();

  }

}
