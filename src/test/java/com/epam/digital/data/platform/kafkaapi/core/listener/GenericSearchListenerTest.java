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

import com.epam.digital.data.platform.kafkaapi.core.exception.ProcedureErrorException;
import com.epam.digital.data.platform.kafkaapi.core.model.ValidationResult;
import com.epam.digital.data.platform.kafkaapi.core.searchhandler.AbstractSearchHandler;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.GenericSearchListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntityContains;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.RequestContext;
import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.SecurityContext;
import com.epam.digital.data.platform.model.core.kafka.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;

import static com.epam.digital.data.platform.model.core.kafka.Status.INVALID_SIGNATURE;
import static com.epam.digital.data.platform.model.core.kafka.Status.JWT_INVALID;
import static com.epam.digital.data.platform.model.core.kafka.Status.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = GenericSearchListenerTestImpl.class)
class GenericSearchListenerTest {

  public static final String KEY = "datafactory-key";
  @MockBean
  AbstractSearchHandler<MockEntityContains, MockEntity> searchHandler;
  @MockBean
  InputValidationService inputValidationService;
  @MockBean
  ResponseMessageCreator responseMessageCreator;
  @Autowired
  GenericSearchListenerTestImpl instance;

  @Captor
  private ArgumentCaptor<Response<List<MockEntity>>> responseCaptor;

  @BeforeEach
  void init() {
    when(inputValidationService.validate(any(), any())).thenReturn(new ValidationResult(true));

    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(new Response<>()).build());
  }

  @Test
  void shouldSearchInHandler() {
    var c = mockResult();

    given(searchHandler.search(any(Request.class))).willReturn(List.of(c));
    var mockResponse = new Response<>();
    mockResponse.setPayload(List.of(c));
    given(responseMessageCreator.createMessageByPayloadSize(any()))
        .willReturn(MessageBuilder.withPayload(mockResponse).build());

    var actualMessage = instance.search(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseCaptor.capture());
    var plainResponse = responseCaptor.getValue();
    assertThat(plainResponse.getStatus()).isEqualTo(SUCCESS);
    assertThat(plainResponse.getPayload()).hasSize(1);
    assertThat(plainResponse.getPayload().get(0)).isEqualTo(c);

    assertThat(actualMessage.getPayload()).isEqualTo(mockResponse);
  }

  @Test
  @DisplayName("Check if response is valid when DB error for search")
  void procedureErrorOnSearch() {
    doThrow(new ProcedureErrorException("")).when(searchHandler).search(any());

    instance.search(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseCaptor.capture());
    var response = responseCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.PROCEDURE_ERROR);
  }

  @Test
  @DisplayName("Check if response is valid when NPE for search")
  void nullPointerExceptionOnSearch() {
    doThrow(new NullPointerException("")).when(searchHandler).search(any());

    instance.search(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseCaptor.capture());
    var response = responseCaptor.getValue();
    assertThat(response.getStatus()).isEqualTo(Status.OPERATION_FAILED);
  }

  @Test
  void shouldReturnInvalidSignatureStatus() {
    when(inputValidationService.validate(any(), any()))
        .thenReturn(new ValidationResult(false, INVALID_SIGNATURE));
    var mockResponse = new Response<>();
    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(mockResponse).build());

    var actualMessage = instance.search(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseCaptor.capture());
    var plainResponse = responseCaptor.getValue();
    assertThat(plainResponse.getStatus()).isEqualTo(INVALID_SIGNATURE);

    assertThat(actualMessage.getPayload()).isEqualTo(mockResponse);
  }

  @Test
  void shouldReturnInvalidJwtStatus() {
    when(inputValidationService.validate(any(), any()))
        .thenReturn(new ValidationResult(false, JWT_INVALID));
    var mockResponse = new Response<>();
    when(responseMessageCreator.createMessageByPayloadSize(any()))
        .thenReturn(MessageBuilder.withPayload(mockResponse).build());

    var actualMessage = instance.search(KEY, mockRequest());

    verify(responseMessageCreator).createMessageByPayloadSize(responseCaptor.capture());
    var plainResponse = responseCaptor.getValue();
    assertThat(plainResponse.getStatus()).isEqualTo(JWT_INVALID);

    assertThat(actualMessage.getPayload()).isEqualTo(mockResponse);
  }

  private MockEntityContains mockSc() {
    return new MockEntityContains();
  }

  private MockEntity mockResult() {
    var c = new MockEntity();
    c.setPersonFullName("Some Full Name");
    return c;
  }

  private Request<MockEntityContains> mockRequest() {
    return new Request<>(mockSc(), new RequestContext(), new SecurityContext());
  }
}
