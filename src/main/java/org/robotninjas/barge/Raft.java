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

import com.google.inject.Guice;
import com.google.inject.Injector;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class Raft {

  public static void main(String[] args) throws UnknownHostException {

    int port = Integer.parseInt(args[0]);

    InetSocketAddress saddr = new InetSocketAddress("127.0.0.1", port);
    Injector injector = Guice.createInjector(new RaftModule(saddr, 1000));
    RaftService service = injector.getInstance(RaftService.class);
    service.startAsync().awaitRunning();

  }

}
