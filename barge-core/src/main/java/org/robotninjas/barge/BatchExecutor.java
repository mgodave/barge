package org.robotninjas.barge;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.jetlang.core.BatchExecutorImpl;
import org.jetlang.core.EventReader;
import org.slf4j.MDC;

import java.util.Map;

class BatchExecutor extends BatchExecutorImpl {

  Map contextMap = Maps.newHashMap();

  @Override
  public void execute(EventReader toExecute) {

    Optional<Map> oldContext = Optional.fromNullable(MDC.getCopyOfContextMap());
    MDC.setContextMap(contextMap);

    super.execute(toExecute);

    contextMap = MDC.getCopyOfContextMap();
    if (oldContext.isPresent()) {
      MDC.setContextMap(oldContext.get());
    }

  }

}
