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
