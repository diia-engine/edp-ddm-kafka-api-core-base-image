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

package com.epam.digital.data.platform.kafkaapi.core.audit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.aspect.AuditAspect;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.CreateCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.DeleteCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.UpdateCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.GenericCreateCommandListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.GenericDeleteCommandListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.GenericUpdateCommandListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.searchhandler.AbstractSearchHandler;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.support.MessageBuilder;

@Import({AopAutoConfiguration.class})
@SpringBootTest(
    classes = {
        AuditAspect.class,
        KafkaAuditProcessor.class,
        GenericCreateCommandListenerTestImpl.class,
        GenericUpdateCommandListenerTestImpl.class,
        GenericDeleteCommandListenerTestImpl.class
    })
@MockBean(DatabaseAuditProcessor.class)
@MockBean(AbstractSearchHandler.class)
@MockBean(CreateCommandHandlerTestImpl.class)
@MockBean(UpdateCommandHandlerTestImpl.class)
@MockBean(DeleteCommandHandlerTestImpl.class)
class AuditKafkaEventsAspectTest {

  @Autowired
  private GenericCreateCommandListenerTestImpl createCommandListener;
  @Autowired
  private GenericUpdateCommandListenerTestImpl updateCommandListener;
  @Autowired
  private GenericDeleteCommandListenerTestImpl deleteCommandListener;

  @MockBean
  private KafkaEventsFacade kafkaEventsFacade;
  @MockBean
  private InputValidationService inputValidationService;
  @MockBean
  private ResponseMessageCreator responseMessageCreator;

  @BeforeEach
  void beforeEach() {
    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(new Response<>()).build());
  }

  @Test
  void expectAuditAspectBeforeAndAfterCreateMethodWhenNoException() {

    createCommandListener.create("", new Request<>());

    verify(kafkaEventsFacade, times(2))
        .sendKafkaAudit(any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterCreateMethodWhenAnyException() {
    when(inputValidationService.validate(any(), any())).thenThrow(new RuntimeException());

    createCommandListener.create("", new Request<>());

    verify(kafkaEventsFacade, times(2))
        .sendKafkaAudit(any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterUpdateMethodWhenNoException() {

    updateCommandListener.update("", new Request<>());

    verify(kafkaEventsFacade, times(2))
        .sendKafkaAudit(any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterUpdateMethodWhenAnyException() {
    when(inputValidationService.validate(any(), any())).thenThrow(new RuntimeException());

    updateCommandListener.update("", new Request<>());

    verify(kafkaEventsFacade, times(2))
        .sendKafkaAudit(any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterDeleteMethodWhenNoException() {

    deleteCommandListener.delete("", new Request<>());

    verify(kafkaEventsFacade, times(2))
        .sendKafkaAudit(any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterDeleteMethodWhenAnyException() {
    when(inputValidationService.validate(any(), any())).thenThrow(new RuntimeException());

    deleteCommandListener.delete("", new Request<>());

    verify(kafkaEventsFacade, times(2))
        .sendKafkaAudit(any(), any(), any(), any(), any(), any());
  }
}
