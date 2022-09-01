/*
 * Copyright 2021 EPAM Systems.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.digital.data.platform.kafkaapi.core.aspect;

import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.epam.digital.data.platform.starter.actuator.livenessprobe.LivenessStateHandler;
import java.util.EnumSet;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class LivenessStateCheckAspect implements KafkaResponseListenerAspect {

  private static final EnumSet<Status> LIVENESS_UNHEALTHY_STATUSES = EnumSet.of(
      Status.THIRD_PARTY_SERVICE_UNAVAILABLE, Status.INVALID_SIGNATURE,
      Status.INTERNAL_CONTRACT_VIOLATION, Status.PROCEDURE_ERROR);

  private final LivenessStateHandler livenessStateHandler;

  public LivenessStateCheckAspect(LivenessStateHandler livenessStateHandler) {
    this.livenessStateHandler = livenessStateHandler;
  }

  @Override
  public void handleKafkaResponse(Message<? extends Response> response) {
    livenessStateHandler
        .handleResponse(response.getPayload().getStatus(), LIVENESS_UNHEALTHY_STATUSES::contains);
  }
}
