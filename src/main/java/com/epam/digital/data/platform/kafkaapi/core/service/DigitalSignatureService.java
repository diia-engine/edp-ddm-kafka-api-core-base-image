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

import com.epam.digital.data.platform.dso.api.dto.VerificationRequestDto;
import com.epam.digital.data.platform.dso.api.dto.VerificationResponseDto;
import com.epam.digital.data.platform.dso.client.DigitalSealRestClient;
import com.epam.digital.data.platform.dso.client.exception.BadRequestException;
import com.epam.digital.data.platform.dso.client.exception.InternalServerErrorException;
import com.epam.digital.data.platform.integration.ceph.exception.CephCommunicationException;
import com.epam.digital.data.platform.integration.ceph.exception.MisconfigurationException;
import com.epam.digital.data.platform.integration.ceph.service.CephService;
import com.epam.digital.data.platform.kafkaapi.core.exception.ExternalCommunicationException;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import feign.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

@Component
public class DigitalSignatureService {

  private final Logger log = LoggerFactory.getLogger(DigitalSignatureService.class);

  private final CephService datafactoryCephService;
  private final String cephBucketName;
  private final DigitalSealRestClient digitalSealRestClient;
  private final ObjectMapper objectMapper;

  private final boolean isEnabled;

  public DigitalSignatureService(
      CephService datafactoryCephService,
      @Value("${ceph.bucket}") String cephBucketName,
      DigitalSealRestClient digitalSealRestClient,
      ObjectMapper objectMapper,
      @Value("${data-platform.kafka-request.validation.enabled}") boolean isEnabled) {
    this.datafactoryCephService = datafactoryCephService;
    this.cephBucketName = cephBucketName;
    this.digitalSealRestClient = digitalSealRestClient;
    this.objectMapper = objectMapper;
    this.isEnabled = isEnabled;
  }

  public <O> boolean isSealValid(String key, O input) {
    if (!isEnabled) {
      return true;
    }

    if (key == null) {
      throw new ExternalCommunicationException(
          "Required kafka header is missing",
          new MessageHandlingException(new GenericMessage<>("Required kafka header is missing")),
          Status.INTERNAL_CONTRACT_VIOLATION);
    }

    String signature = getSignature(key);
    return verify(signature, serialize(input));
  }

  private String getSignature(String key) {
    try {
      log.info("Reading Signature from Ceph");
      return
          datafactoryCephService
              .getAsString(cephBucketName, key)
              .orElseThrow(
                  () ->
                      new ExternalCommunicationException(
                          "Digital signature does not found in ceph",
                          Status.INTERNAL_CONTRACT_VIOLATION));
    } catch (CephCommunicationException e) {
      throw new ExternalCommunicationException(
          "Exception while communication with ceph", e, Status.THIRD_PARTY_SERVICE_UNAVAILABLE);
    } catch (MisconfigurationException e) {
      throw new ExternalCommunicationException(
          "Incorrect Ceph configuration", e, Status.INTERNAL_CONTRACT_VIOLATION);
    }
  }

  private boolean verify(String signature, String data) {
    try {
      log.info("Verifying Signature");
      VerificationResponseDto responseDto =
          digitalSealRestClient.verify(new VerificationRequestDto(signature, data));
      return responseDto.isValid();
    } catch (BadRequestException e) {
      throw new ExternalCommunicationException(
          "Call to external digital signature service violates an internal contract",
          e,
          Status.INTERNAL_CONTRACT_VIOLATION);
    } catch (InternalServerErrorException e) {
      throw new ExternalCommunicationException(
          "External digital signature service has internal server error",
          e,
          Status.THIRD_PARTY_SERVICE_UNAVAILABLE);
    } catch (RetryableException e) {
      throw new ExternalCommunicationException(
          "External digital signature service not responding",
          e,
          Status.THIRD_PARTY_SERVICE_UNAVAILABLE);
    }
  }

  private <T> String serialize(T object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Couldn't serialize object", e);
    }
  }
}
