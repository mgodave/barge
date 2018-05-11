package org.robotninjas.barge;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.jetlang.core.BatchExecutorImpl;
import org.jetlang.core.EventReader;
import org.slf4j.MDC;

class BatchExecutor extends BatchExecutorImpl {

  Map contextMap = Maps.newHashMap();

  @Override
  public void execute(EventReader toExecute) {

    Optional<Map> oldContext = Optional.ofNullable(MDC.getCopyOfContextMap());
    MDC.setContextMap(contextMap);

    super.execute(toExecute);

    contextMap = MDC.getCopyOfContextMap();
    oldContext.ifPresent(MDC::setContextMap);

  }

}
