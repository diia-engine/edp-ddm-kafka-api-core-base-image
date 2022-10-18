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

package com.epam.digital.data.platform.kafkaapi.core.listener.impl;

import static com.epam.digital.data.platform.kafkaapi.core.util.Header.DIGITAL_SEAL;

import com.epam.digital.data.platform.kafkaapi.core.audit.AuditableListener;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.CreateCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.listener.GenericCreateCommandListener;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.kafkaapi.core.util.Operation;
import com.epam.digital.data.platform.model.core.kafka.EntityId;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Response;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;

@TestComponent
public class GenericCreateCommandListenerTestImpl extends GenericCreateCommandListener<MockEntity, EntityId> {

  protected GenericCreateCommandListenerTestImpl(CreateCommandHandlerTestImpl commandHandler) {
    super(commandHandler);
  }

  @AuditableListener(Operation.CREATE)
  @Override
  @KafkaListener
  @SendTo
  public Message<Response<EntityId>> create(
      @Header(name = DIGITAL_SEAL, required = false) String key, Request<MockEntity> input) {
    return super.create(key, input);
  }
}
