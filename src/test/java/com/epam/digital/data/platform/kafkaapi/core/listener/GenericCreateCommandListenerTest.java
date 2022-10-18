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

package com.epam.digital.data.platform.kafkaapi.core.listener;

import static com.epam.digital.data.platform.model.core.kafka.Status.INVALID_SIGNATURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.CreateCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.exception.ProcedureErrorException;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.GenericCreateCommandListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.model.ValidationResult;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.EntityId;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.Status;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootTest(classes = GenericCreateCommandListenerTestImpl.class)
class GenericCreateCommandListenerTest {

  private static final UUID ENTITY_ID = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
  private static final String KEY = "datafactory-key";

  @MockBean
  CreateCommandHandlerTestImpl mockCommandHandler;
  @MockBean
  InputValidationService inputValidationService;
  @MockBean
  ResponseMessageCreator responseMessageCreator;
  @Autowired
  private GenericCreateCommandListener<MockEntity, EntityId> commandListener;

  @Captor
  private ArgumentCaptor<Response<Void>> responseNoPayloadCaptor;

  @BeforeEach
  void init() {
    when(inputValidationService.validate(any(), any())).thenReturn(new ValidationResult(true));

    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(new Response<>()).build());
  }

  @Test
  @DisplayName("Check if status is correct if entity saved")
  void happyWritePath() {
    commandListener.create(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseNoPayloadCaptor.capture());
    var response = responseNoPayloadCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.CREATED);
  }

  @Test
  @DisplayName("Check if response is valid when DB error for create")
  void procedureErrorOnWrite() {
    doThrow(new ProcedureErrorException("")).when(mockCommandHandler).save(any());

    commandListener.create("", mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseNoPayloadCaptor.capture());
    var response = responseNoPayloadCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.PROCEDURE_ERROR);
  }

  @Test
  @DisplayName("Check if response is valid when NPE for create")
  void nullPointerExceptionOnWrite() {
    doThrow(new NullPointerException("")).when(mockCommandHandler).save(any());

    commandListener.create("", mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseNoPayloadCaptor.capture());
    var response = responseNoPayloadCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.OPERATION_FAILED);
  }

  @Test
  void shouldReturnErrorStatusFromValidation() {
    when(inputValidationService.validate(any(), any()))
        .thenReturn(new ValidationResult(false, INVALID_SIGNATURE));
    var mockResponse = new Response<>();
    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(mockResponse).build());

    var actualMessage = commandListener.create(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseNoPayloadCaptor.capture());
    var plainResponse = responseNoPayloadCaptor.getValue();
    assertThat(plainResponse.getStatus()).isEqualTo(INVALID_SIGNATURE);
    assertThat(plainResponse.getDetails()).isNull();

    assertThat(actualMessage.getPayload()).isEqualTo(mockResponse);
  }

  private Request<MockEntity> mockRequest() {
    MockEntity mock = new MockEntity();
    Request<MockEntity> request = new Request<>();

    mock.setConsentId(ENTITY_ID);
    mock.setPersonFullName("stub");
    request.setPayload(mock);

    return request;
  }
}
