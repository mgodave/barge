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

import com.google.inject.assistedinject.Assisted;
import org.robotninjas.barge.Replica;

import javax.annotation.Nonnull;

interface ReplicaManagerFactory {

  @Nonnull
  ReplicaManager create(@Assisted("term") long term, @Assisted("nextIndex") long nextIndex,
                        @Assisted("remote") @Nonnull Replica remote, @Assisted("self") @Nonnull Replica self);

}
