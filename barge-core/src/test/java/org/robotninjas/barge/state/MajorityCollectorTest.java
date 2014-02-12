package org.robotninjas.barge.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;
import static org.robotninjas.barge.state.MajorityCollectorTest.BooleanIdentity.Identity;

public class MajorityCollectorTest {

  @Test
  public void testAllFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFailedFuture(new Exception())
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityCompletedSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityCompletedFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajorityNotSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testMajoritySuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testAllSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testAllNotSuccess() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityTrueAllCompleted() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertTrue(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityFalseAllCompleted() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.FALSE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    assertFalse(Futures.getUnchecked(collector));

  }

  @Test
  public void testBareMajorityTrueOthersFailed() {

    List<ListenableFuture<Boolean>> responses = Lists.newArrayList(
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFailedFuture(new Exception()),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE),
      Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

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

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

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
      f1, f2, Futures.<Boolean>immediateFuture(Boolean.TRUE)
    );

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

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

    ListenableFuture<Boolean> collector = majorityResponse(responses, Identity);

    f1.setException(new Exception());
    assertFalse(collector.isDone());

    f3.set(Boolean.TRUE);
    assertFalse(collector.isDone());

    f2.setException(new Exception());
    assertTrue(collector.isDone());

    assertFalse(Futures.getUnchecked(collector));

  }

  static enum BooleanIdentity implements Predicate<Boolean> {

    Identity;

    @Override
    public boolean apply(@Nullable Boolean input) {
      return input;
    }

  }


}
