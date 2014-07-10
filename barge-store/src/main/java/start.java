import com.google.common.collect.Lists;

import org.robotninjas.barge.store.RaftStoreServer;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;


/**
 * Wrapper over {@link org.robotninjas.barge.store.RaftStoreServer} that forks a new instance in the background.
 * <p>
 *   This main class is used to run barge in the background without relying on native mechanism: It forks actual main to another
 *   process and redirects stdout and stderr to some files.
 * </p>
 */
public class start {

  private static final String JAVA = System.getProperty("java.home", "java");
  private static final String CLASSPATH = System.getProperty("java.class.path", "barge-store.jar");

  public static void main(String[] args) throws IOException {
    ArrayList<String> arguments = Lists.newArrayList(JAVA + "/bin/java", "-cp", CLASSPATH, RaftStoreServer.class.getName());

    Collections.addAll(arguments, args);

    long timestamp = System.currentTimeMillis();
    File stdout = new File(".barge." + timestamp + ".out");
    File stderr = new File(".barge." + timestamp + ".err");

    ProcessBuilder builder = new ProcessBuilder(arguments).redirectError(stderr)
        .redirectOutput(stdout)
        .redirectInput(ProcessBuilder.Redirect.PIPE);

    builder.start();
  }
}
