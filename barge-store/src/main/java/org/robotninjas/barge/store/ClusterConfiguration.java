package org.robotninjas.barge.store;

import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.List;


/**
 * Utilities for configuring cluster using some external source of configuration (eg. a file).
 */
public class ClusterConfiguration {

  public URI[] readConfiguration(File clusterConfigurationFile) throws IOException, URISyntaxException {
    List<URI> uris = Lists.newArrayList();

    int lineNumber = 1;

    for (String line : CharStreams.readLines(new FileReader(clusterConfigurationFile))) {
      String[] pair = line.split("=");

      if (pair.length != 2)
        throw new IOException("Invalid cluster configuration at line " + lineNumber);

      uris.add(Integer.parseInt(pair[0].trim()), new URI(pair[1].trim()));
    }

    return uris.toArray(new URI[uris.size()]);
  }
}
