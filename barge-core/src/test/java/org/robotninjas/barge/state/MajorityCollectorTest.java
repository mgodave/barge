package org.robotninjas.barge.state;

import static java.util.function.Function.identity;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.List;
import org.junit.Test;

public class MajorityCollectorTest {

  @Test
  public void testAllFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFailedFuture(new Exception())
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityCompletedSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityCompletedFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityNotSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajoritySuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testAllSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testAllNotSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityTrueAllCompleted() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityFalseAllCompleted() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.FALSE),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityTrueOthersFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFailedFuture(new Exception()),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE),
      Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testFireOnceMajoritySuccess() {

    SettableFuture<Boolean> f1 = SettableFuture.create();
    SettableFuture<Boolean> f2 = SettableFuture.create();
    SettableFuture<Boolean> f3 = SettableFuture.create();

    List<SettableFuture<Boolean>> responses = Lists.newArrayList(
      f1, f2, f3
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    f1.set(Boolean.TRUE);
    assertFalse(collector.isDone());

    f2.set(Boolean.TRUE);
    assertTrue(collector.isDone());

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testFireOnceMajorityFailed1() {

    SettableFuture<Boolean> f1 = SettableFuture.create();
    SettableFuture<Boolean> f2 = SettableFuture.create();

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      f1, f2, Futures.immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    f1.setException(new Exception());
    assertFalse(collector.isDone());

    f2.setException(new Exception());
    assertTrue(collector.isDone());

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testFireOnceMajorityFailed() {

    SettableFuture<Boolean> f1 = SettableFuture.create();
    SettableFuture<Boolean> f2 = SettableFuture.create();
    SettableFuture<Boolean> f3 = SettableFuture.create();
    ListenableFuture<Boolean> f4 = Futures.immediateFuture(Boolean.TRUE);

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      f1, f2, f3, f4
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, input -> input);

    f1.setException(new Exception());
    assertFalse(collector.isDone());

    f3.set(Boolean.TRUE);
    assertFalse(collector.isDone());

    f2.setException(new Exception());
    assertTrue(collector.isDone());

    assertFalse(Futures.getUnchecked(collector));

  }

}
