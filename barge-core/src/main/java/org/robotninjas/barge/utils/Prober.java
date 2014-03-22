/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
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
package org.robotninjas.barge.utils;

import java.util.concurrent.Callable;

import static com.google.common.base.Throwables.propagate;

/**
 * Repeatedly probe for some condition to become true.
 */
public class Prober {

  private Callable<Boolean> condition;

  public Prober(Callable<Boolean> condition) {
    this.condition = condition;
  }

  /**
   * Start probing, waiting for {@code condition} to become true.
   * <p/>
   * <p>This method is blocking: It returns when condition becomes true or throws an exception if timeout is reached.</p>
   *
   * @param timeoutInMs wait time in milliseconds
   */
  public void probe(long timeoutInMs) {
    long start = System.nanoTime();

    try {
      while (!condition.call() && (elapsedMs(start) < timeoutInMs)) {
        Thread.yield();
      }

      if (!condition.call()) {
        throw new IllegalStateException("probe condition not true after " + timeoutInMs);
      }
    } catch (Exception e) {
      throw propagate(e);
    }
  }

  private long elapsedMs(long start) {
    return (System.nanoTime() - start) / 1000000;
  }

}
