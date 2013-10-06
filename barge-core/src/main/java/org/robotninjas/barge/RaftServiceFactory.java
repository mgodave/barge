package org.robotninjas.barge;

import com.google.inject.assistedinject.Assisted;

import javax.annotation.Nonnull;

interface RaftServiceFactory {

  RaftService create(@Nonnull @Assisted StateMachine stateMachine);

}
