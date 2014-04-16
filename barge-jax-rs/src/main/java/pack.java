/**
 *  Copyright Murex S.A.S., 2003-2014. All Rights Reserved.
 *
 *  This software program is proprietary and confidential to Murex S.A.S and its affiliates ("Murex") and, without limiting the generality of the foregoing reservation of rights, shall not be accessed, used, reproduced or distributed without the
 *  express prior written consent of Murex and subject to the applicable Murex licensing terms. Any modification or removal of this copyright notice is expressly prohibited.
 */

import com.google.common.io.ByteStreams;

import java.io.*;

import java.net.URL;


/**
 * Simple auto-packing application.
 */
public class pack {

  public static void main(String[] args) throws IOException {
    File script = new File("barge.sh");

    URL url = pack.class.getProtectionDomain().getCodeSource().getLocation();

    OutputStream fos = null;

    try {
      fos = new FileOutputStream(script);

      InputStream headerStream = pack.class.getClassLoader().getResourceAsStream("script-header");
      InputStream inputStream = url.openStream();

      ByteStreams.copy(headerStream, fos);
      ByteStreams.copy(inputStream, fos);

      script.setExecutable(true);
    } finally {

      if (fos != null)
        fos.close();
    }

  }
}
