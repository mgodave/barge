package org.robotninjas.barge.state;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

public class MajorityCollectorTest {

  @Test
  public void testAllFailed() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      failedFuture(new Exception()),
      failedFuture(new Exception()),
      failedFuture(new Exception())
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityFailed() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      failedFuture(new Exception()),
      failedFuture(new Exception()),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityCompletedSuccess() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      failedFuture(new Exception()),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityCompletedFailed() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      failedFuture(new Exception()),
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityNotSuccess() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajoritySuccess() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testAllSuccess() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testAllNotSuccess() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityTrueAllCompleted() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityFalseAllCompleted() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.FALSE),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityTrueOthersFailed() {

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      failedFuture(new Exception()),
      failedFuture(new Exception()),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE),
      completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testFireOnceMajoritySuccess() {

    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
    CompletableFuture<Boolean> f3 = new CompletableFuture<>();

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      f1, f2, f3
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    f1.complete(Boolean.TRUE);
    assertFalse(collector.isDone());

    f2.complete(Boolean.TRUE);
    assertTrue(collector.isDone());

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testFireOnceMajorityFailed1() {

    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      f1, f2, completedFuture(Boolean.TRUE)
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    f1.completeExceptionally(new Exception());
    assertFalse(collector.isDone());

    f2.completeExceptionally(new Exception());
    assertTrue(collector.isDone());

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testFireOnceMajorityFailed() {

    CompletableFuture<Boolean> f1 = new CompletableFuture<>();
    CompletableFuture<Boolean> f2 = new CompletableFuture<>();
    CompletableFuture<Boolean> f3 = new CompletableFuture<>();
    CompletableFuture<Boolean> f4 = completedFuture(Boolean.TRUE);

    List<CompletableFuture<Boolean>> responses = Lists.newArrayList(
      f1, f2, f3, f4
    );

    CompletableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    f1.completeExceptionally(new Exception());
    assertFalse(collector.isDone());

    f3.complete(Boolean.TRUE);
    assertFalse(collector.isDone());

    f2.completeExceptionally(new Exception());
    assertTrue(collector.isDone());

    assertFalse(Futures.getUnchecked(collector));

  }

  private <U> CompletableFuture<U> failedFuture(Throwable t) {
    CompletableFuture<U> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

}
