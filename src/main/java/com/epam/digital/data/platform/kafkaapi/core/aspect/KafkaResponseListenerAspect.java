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
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.messaging.Message;

public interface KafkaResponseListenerAspect {

  @Pointcut("@annotation(org.springframework.kafka.annotation.KafkaListener)")
  default void kafkaListenerPointcut() {}

  @AfterReturning(pointcut = "kafkaListenerPointcut()", returning = "response")
  void handleKafkaResponse(Message<? extends Response> response);
}
