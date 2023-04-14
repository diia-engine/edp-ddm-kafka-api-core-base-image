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

package com.epam.digital.data.platform.kafkaapi.core.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.dso.api.dto.VerificationRequestDto;
import com.epam.digital.data.platform.dso.api.dto.VerificationResponseDto;
import com.epam.digital.data.platform.dso.client.DigitalSealRestClient;
import com.epam.digital.data.platform.dso.client.exception.BadRequestException;
import com.epam.digital.data.platform.dso.client.exception.InternalServerErrorException;
import com.epam.digital.data.platform.dso.client.exception.InvalidSignatureException;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.kafkaapi.core.config.JooqTestConfig;
import com.epam.digital.data.platform.kafkaapi.core.exception.ExternalCommunicationException;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = JooqTestConfig.class)
class DigitalSignatureServiceTest {

  private static final String BUCKET = "bucket";
  private static final String SIGNATURE = "signature";
  private static final String KEY = "datafactory-key";
  @Mock
  private CephService cephService;
  @Mock
  private DigitalSealRestClient digitalSealRestClient;
  @Autowired
  private ObjectMapper objectMapper;
  private DigitalSignatureService digitalSignatureService;
  private Request<MockEntity> request;

  @BeforeEach
  void init() {
    when(cephService.getAsString(BUCKET, KEY)).thenReturn(Optional.of(SIGNATURE));
    digitalSignatureService = new DigitalSignatureService(cephService, BUCKET,
        digitalSealRestClient, objectMapper, true);
    request = new Request<>(getMockPayload(), null, null);
    when(digitalSealRestClient.verify(any()))
        .thenReturn(new VerificationResponseDto(true, null));
  }

  @Test
  void kafkaHeaderAbsentThrowsException() {
    assertThrows(ExternalCommunicationException.class,
        () -> digitalSignatureService.isSealValid(null, request));
  }

  @Test
  void checkThatTheCorrectArgumentsArePassedToTheVerifyFunction() throws JsonProcessingException {
    digitalSignatureService.isSealValid(KEY, request);

    ArgumentCaptor<VerificationRequestDto> requestCaptor = ArgumentCaptor
        .forClass(VerificationRequestDto.class);
    verify(digitalSealRestClient).verify(requestCaptor.capture());

    assertEquals(SIGNATURE, requestCaptor.getValue().getSignature());
    assertEquals(objectMapper.writeValueAsString(request), requestCaptor.getValue().getData());
  }

  @Test
  void cephCommunicationExceptionChangedToExternalCommunicationException() {
    when(cephService.getAsString(any(), any())).thenThrow(CephCommunicationException.class);
    var actualException = assertThrows(ExternalCommunicationException.class,
        () -> digitalSignatureService.isSealValid(KEY, request));
    assertThat(actualException.getKafkaResponseStatus()).isEqualTo(
        Status.THIRD_PARTY_SERVICE_UNAVAILABLE);
  }

  @Test
  void externalCommunicationExceptionWhenNotFoundCephContent() {
    when(cephService.getAsString(any(), any())).thenReturn(Optional.empty());
    var actualException = assertThrows(ExternalCommunicationException.class,
        () -> digitalSignatureService.isSealValid(KEY, request));
    assertThat(actualException.getKafkaResponseStatus()).isEqualTo(
        Status.INTERNAL_CONTRACT_VIOLATION);
  }

  @Test
  void misconfigurationExceptionChangedToExternalCommunicationException() {
    when(cephService.getAsString(any(), any())).thenThrow(MisconfigurationException.class);
    assertThrows(ExternalCommunicationException.class,
        () -> digitalSignatureService.isSealValid(KEY, request));
  }

  @Test
  void badRequestExceptionChangedToExternalCommunicationException() {
    when(digitalSealRestClient.verify(any())).thenThrow(BadRequestException.class);
    assertThrows(ExternalCommunicationException.class,
        () -> digitalSignatureService.isSealValid(KEY, request));
  }

  @Test
  void internalServerErrorExceptionChangedToExternalCommunicationException() {
    when(digitalSealRestClient.verify(any())).thenThrow(InternalServerErrorException.class);
    assertThrows(ExternalCommunicationException.class,
        () -> digitalSignatureService.isSealValid(KEY, request));
  }

  @Test
  void invalidSignatureExceptionChangedToFalse() {
    when(digitalSealRestClient.verify(any())).thenThrow(InvalidSignatureException.class);
    assertThat(digitalSignatureService.isSealValid(KEY, request)).isFalse();
  }

  @Test
  void shouldReturnTrueWhenValidationDisabled() {
    digitalSignatureService = new DigitalSignatureService(cephService, BUCKET,
        digitalSealRestClient, objectMapper, false);

    var actual = digitalSignatureService.isSealValid(KEY, request);

    assertThat(actual).isTrue();
  }

  @Test
  void jsonProcessingExceptionChangedToIllegalStateException() {
    digitalSignatureService = new DigitalSignatureService(cephService, BUCKET,
        digitalSealRestClient, new ObjectMapper(), true);

    assertThrows(IllegalStateException.class,
        () -> digitalSignatureService.isSealValid(KEY, request));
  }

  private MockEntity getMockPayload() {
    MockEntity mockEntity = new MockEntity();
    mockEntity.setConsentId(UUID.fromString("123e4567-e89b-12d3-a456-426655440000"));
    mockEntity.setConsentDate(LocalDateTime
        .parse("2021-02-06T12:33:20.111Z",
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")));
    mockEntity.setPersonFullName("Full name");
    mockEntity.setPersonPassNumber("АА123456");
    return mockEntity;
  }
}
