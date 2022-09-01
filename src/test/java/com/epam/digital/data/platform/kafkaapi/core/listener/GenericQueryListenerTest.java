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
import static com.epam.digital.data.platform.model.core.kafka.Status.JWT_INVALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.exception.ProcedureErrorException;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.GenericQueryListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.model.ValidationResult;
import com.epam.digital.data.platform.kafkaapi.core.queryhandler.impl.QueryHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
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
import java.util.Optional;

@SpringBootTest(classes = GenericQueryListenerTestImpl.class)
class GenericQueryListenerTest {

  private static final UUID ENTITY_ID = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
  private static final String KEY = "datafactory-key";

  @MockBean
  QueryHandlerTestImpl mockQueryHandler;
  @MockBean
  InputValidationService inputValidationService;
  @MockBean
  ResponseMessageCreator responseMessageCreator;
  @Autowired
  private GenericQueryListenerTestImpl queryListener;

  @Captor
  private ArgumentCaptor<Response<MockEntity>> responseWithPayloadCaptor;

  @BeforeEach
  void init() {
    when(inputValidationService.validate(any(), any())).thenReturn(new ValidationResult(true));

    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(new Response<>()).build());
  }

  @Test
  @DisplayName("Check if response is failed on record not found")
  void notFound() {
    when(mockQueryHandler.findById(any())).thenReturn(Optional.empty());

    queryListener.read(KEY, mockInput());

    verify(responseMessageCreator).createMessageByPayloadSize(responseWithPayloadCaptor.capture());
    var response = responseWithPayloadCaptor.getValue();
    assertThat(response.getPayload()).isNull();
    assertThat(response.getStatus()).isEqualTo(Status.NOT_FOUND);
  }

  @Test
  @DisplayName("Check if response is valid when DB error for read")
  void procedureErrorOnRead() {
    doThrow(new ProcedureErrorException("")).when(mockQueryHandler).findById(any());

    queryListener.read(KEY, mockInput());

    verify(responseMessageCreator).createMessageByPayloadSize(responseWithPayloadCaptor.capture());
    var response = responseWithPayloadCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.PROCEDURE_ERROR);
  }

  @Test
  @DisplayName("Check if response is valid when NPE for read")
  void nullPointerExceptionOnWrite() {
    doThrow(new NullPointerException("")).when(mockQueryHandler).findById(any());

    queryListener.read(KEY, mockInput());

    verify(responseMessageCreator).createMessageByPayloadSize(responseWithPayloadCaptor.capture());
    var response = responseWithPayloadCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.OPERATION_FAILED);
  }

  @Test
  @DisplayName("Check if status is correct if entity found")
  void happyReadPath() {
    MockEntity mock = new MockEntity();
    mock.setConsentId(ENTITY_ID);
    mock.setPersonFullName("stub");
    when(mockQueryHandler.findById(any())).thenReturn(Optional.of(mock));

    var mockResponse = new Response<>();
    mockResponse.setPayload(mock);
    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(mockResponse).build());

    Response<MockEntity> actualResponse = queryListener.read(KEY, mockInput()).getPayload();

    verify(responseMessageCreator).createMessageByPayloadSize(responseWithPayloadCaptor.capture());
    var plainResponse = responseWithPayloadCaptor.getValue();
    assertThat(plainResponse.getPayload()).isEqualTo(mock);
    assertThat(plainResponse.getStatus()).isEqualTo(Status.SUCCESS);

    assertThat(actualResponse).isEqualTo(mockResponse);
  }

  @Test
  void shouldReturnInvalidSignatureStatus() {
    when(inputValidationService.validate(any(), any()))
        .thenReturn(new ValidationResult(false, INVALID_SIGNATURE));
    var mockResponse = new Response<>();
    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(mockResponse).build());

    var actualMessage = queryListener.read(KEY, mockInput());

    verify(responseMessageCreator).createMessageByPayloadSize(responseWithPayloadCaptor.capture());
    var plainResponse = responseWithPayloadCaptor.getValue();
    assertThat(plainResponse.getStatus()).isEqualTo(INVALID_SIGNATURE);

    assertThat(actualMessage.getPayload()).isEqualTo(mockResponse);
  }

  @Test
  void expectJwtInvalidStatusOnReadIfValidationNotPassed() {
    when(inputValidationService.validate(any(), any()))
        .thenReturn(new ValidationResult(false, JWT_INVALID));
    MockEntity mock = new MockEntity();
    mock.setConsentId(ENTITY_ID);
    mock.setPersonFullName("stub");
    when(mockQueryHandler.findById(any())).thenReturn(Optional.of(mock));

    queryListener.read(KEY, mockInput());

    verify(responseMessageCreator).createMessageByPayloadSize(responseWithPayloadCaptor.capture());
    var response = responseWithPayloadCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(JWT_INVALID);
    assertThat(response.getDetails()).isNull();
  }

  private Request<UUID> mockInput() {
    return new Request<>(ENTITY_ID, null, null);
  }
}
