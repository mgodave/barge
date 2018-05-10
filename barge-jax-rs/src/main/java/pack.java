import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;


/**
 * Simple auto-packing application.
 */
public class pack {

  public static void main(String[] args) throws IOException {
    File script = new File("barge.sh");

    URL url = pack.class.getProtectionDomain().getCodeSource().getLocation();

    try (OutputStream fos = new FileOutputStream(script)) {

      InputStream headerStream = pack.class.getClassLoader().getResourceAsStream("script-header");
      InputStream inputStream = url.openStream();

      ByteStreams.copy(headerStream, fos);
      ByteStreams.copy(inputStream, fos);

      script.setExecutable(true);
    }

  }
}
