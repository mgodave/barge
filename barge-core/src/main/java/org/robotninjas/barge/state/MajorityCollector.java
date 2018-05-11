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

package org.robotninjas.barge.state;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;


@ThreadSafe
class MajorityCollector<T> implements Consumer<T>, Function<Throwable, T> {

  private final ReentrantLock lock = new ReentrantLock();
  private final Predicate<T> isSuccess;
  private final CompletableFuture<Boolean> future;
  private final int totalNum;
  @GuardedBy("lock")
  private int numSuccess = 0;
  @GuardedBy("lock")
  private int numFailed = 0;

  private MajorityCollector(@Nonnegative int totalNum, @Nonnull Predicate<T> isSuccess, CompletableFuture<Boolean> future) {
    this.totalNum = totalNum;
    this.isSuccess = isSuccess;
    this.future = future;
  }

  @Nonnull
  public static <U> CompletableFuture<Boolean> majorityResponse(@Nonnull List<? extends CompletableFuture<U>> responses, @Nonnull Predicate<U> isSuccess) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    MajorityCollector<U> collector = new MajorityCollector<>(responses.size(), isSuccess, future);
    for (CompletableFuture<U> response : responses) {
      response.thenAccept(collector);
      response.exceptionally(collector);
    }
    if (responses.isEmpty()) {
      collector.checkComplete();
    }
    return future;
  }

  private void checkComplete() {
    if (!future.isDone()) {
      final double half = totalNum / 2.0;
      if (numSuccess > half) {
        future.complete(true);
      } else if (numFailed >= half) {
        future.complete(false);
      }
    }
  }

  @Override
  public void accept(@Nonnull T result) {

    checkNotNull(result);

    lock.lock();
    try {
      if (isSuccess.test(result)) {
        numSuccess++;
      } else {
        numFailed++;
      }
      checkComplete();
    } finally {
      lock.unlock();
    }

  }

  @Override
  public T apply(@Nonnull Throwable t) {
    lock.lock();
    try {
      numFailed++;
      checkComplete();
    } finally {
      lock.unlock();
    }
    return null;
  }

}
