/*
 * Copyright 2023 EPAM Systems.
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

import static com.epam.digital.data.platform.kafkaapi.core.util.Header.DIGITAL_SEAL;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_ACCESS_TOKEN;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_DIGITAL_SIGNATURE;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_DIGITAL_SIGNATURE_DERIVED;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID;
import static com.epam.digital.data.platform.kafkaapi.core.util.SecurityUtils.mockJwt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

import com.epam.digital.data.platform.kafkaapi.core.Main;
import com.epam.digital.data.platform.kafkaapi.core.config.TestConfiguration;
import com.epam.digital.data.platform.kafkaapi.core.impl.commandhandler.TestEntityUpsertCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.impl.listener.TestEntityAsyncDataLoadKafkaListener;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadRequest;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadResponse;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadResult;
import com.epam.digital.data.platform.model.core.kafka.File;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import com.epam.digital.data.platform.storage.file.dto.FileDataDto;
import com.epam.digital.data.platform.storage.file.dto.FileMetadataDto;
import com.epam.digital.data.platform.storage.file.service.FormDataFileStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.PlatformTransactionManager;

@ActiveProfiles("int-test")
@EmbeddedKafka(count = 3)
@SpringBootTest(classes = {
    Main.class
})
@TestConfiguration
@Disabled
public class AsyncDataLoadKafkaListenerIT {

  private static final String ENTITY_NAME = "EntityName";
  private static final String RESULT_VARIABLE = "ResultVariable";
  @Autowired
  private KafkaProperties kafkaProperties;

  @Autowired
  TestEntityAsyncDataLoadKafkaListener listener;

  @Autowired
  ObjectMapper objectMapper;

  @SpyBean
  TestEntityUpsertCommandHandler handler;
  @MockBean
  PlatformTransactionManager transactionManager;

  @MockBean
  @Qualifier("lowcodeFileDataStorageService")
  FormDataFileStorageService lowcodeFileDataStorageService;
  @MockBean
  @Qualifier("datafactoryFileDataStorageService")
  FormDataFileStorageService datafactoryFileDataStorageService;


  @SneakyThrows
  @Test
  void shouldSuccessfullyProcessKafkaMessage() {
    InputStream resourceAsStream1 = AsyncDataLoadKafkaListenerIT.class.getResourceAsStream(
        "/csv/mockEntity.csv");
    InputStream resourceAsStream2 = AsyncDataLoadKafkaListenerIT.class.getResourceAsStream(
        "/csv/mockEntity.csv");
    FileDataDto fileDataDto = FileDataDto.builder()
        .content(resourceAsStream1)
        .build();
    when(lowcodeFileDataStorageService.loadByKey(anyString())).thenReturn(fileDataDto);
    when(datafactoryFileDataStorageService.save(anyString(), any())).thenReturn(
        mock(FileMetadataDto.class));

    var payload = getAsyncDataLoadRequest(
        DigestUtils.sha256Hex(IOUtils.toByteArray(Objects.requireNonNull(resourceAsStream2))));
    var message = MessageBuilder
        .withPayload(payload)
        .copyHeaders(buildHeaders())
        .build();

    var result = listener.asyncDataLoad(message);

    var expectedResultPayload = getResponseMessage(Status.SUCCESS, "OK").getPayload();
    var resultPayload = objectMapper.readValue(result.getPayload(), AsyncDataLoadResponse.class);

    assertEquals(expectedResultPayload.getPayload(), resultPayload.getPayload());
    assertEquals(expectedResultPayload.getDetails(), resultPayload.getDetails());
    assertEquals(expectedResultPayload.getStatus(), resultPayload.getStatus());

    verify(handler, times(2)).upsert(any());
    verify(transactionManager, never()).rollback(any());
    verify(lowcodeFileDataStorageService).loadByKey(anyString());
    verify(datafactoryFileDataStorageService).save(anyString(), any());
  }

  @SneakyThrows
  @ParameterizedTest
  @MethodSource("testArgumentProvider")
  void shouldThrowExceptionWhenInvalidCsv(String file, String details) {
    InputStream resourceAsStream1 = AsyncDataLoadKafkaListenerIT.class.getResourceAsStream(file);
    InputStream resourceAsStream2 = AsyncDataLoadKafkaListenerIT.class.getResourceAsStream(file);
    FileDataDto fileDataDto = FileDataDto.builder()
        .content(resourceAsStream1)
        .build();
    when(lowcodeFileDataStorageService.loadByKey(anyString())).thenReturn(fileDataDto);
    when(datafactoryFileDataStorageService.save(anyString(), any())).thenReturn(
        mock(FileMetadataDto.class));

    var payload = getAsyncDataLoadRequest(
        DigestUtils.sha256Hex(IOUtils.toByteArray(Objects.requireNonNull(resourceAsStream2))));
    var message = MessageBuilder
        .withPayload(payload)
        .copyHeaders(buildHeaders())
        .build();

    var result = listener.asyncDataLoad(message);

    var expectedResultPayload = getResponseMessage(Status.OPERATION_FAILED, details).getPayload();
    var resultPayload = objectMapper.readValue(result.getPayload(), AsyncDataLoadResponse.class);

    assertEquals(expectedResultPayload.getDetails(), resultPayload.getDetails());
    assertEquals(expectedResultPayload.getStatus(), resultPayload.getStatus());

    verify(handler, never()).upsert(any());
    verify(transactionManager).rollback(any());
    verify(lowcodeFileDataStorageService).loadByKey(anyString());
    verify(datafactoryFileDataStorageService).save(anyString(), any());
  }

  static Stream<Arguments> testArgumentProvider() {
    return Stream.of(
        arguments("/csv/mockEntityInvalidCsvFormat.csv", "Exception on parsing csv file content"),
        arguments("/csv/mockEntityInvalidEncoding.csv",
            "Wrong csv file encoding found instead of UTF-8: ISO-8859-1"),
        arguments("/csv/mockEntityInvalidPassFormat.csv", "Failed validation of csv file content")
    );
  }

  @SneakyThrows
  private String getAsyncDataLoadRequest(String checkSum) {
    AsyncDataLoadRequest payload = new AsyncDataLoadRequest();
    File file = new File();
    file.setId("fileId");
    file.setChecksum("fileChecksum");
    File derivedFile = new File();
    derivedFile.setId("derivedFileId");
    derivedFile.setChecksum(checkSum);
    payload.setFile(file);
    payload.setDerivedFile(derivedFile);

    return objectMapper.writeValueAsString(payload);
  }

  private Message<AsyncDataLoadResponse> getResponseMessage(Status status, String details) {
    AsyncDataLoadResult asyncDataLoadResult = new AsyncDataLoadResult();
    asyncDataLoadResult.setResultVariable(RESULT_VARIABLE);
    asyncDataLoadResult.setEntityName(ENTITY_NAME);
    AsyncDataLoadResponse response = new AsyncDataLoadResponse();
    response.setPayload(asyncDataLoadResult);
    response.setDetails(details);
    response.setStatus(status);
    return MessageBuilder.withPayload(response).build();
  }

  @SneakyThrows
  private MessageHeaders buildHeaders() {
    Map<String, Object> map = new HashMap<>();
    map.put(TOPIC, kafkaProperties.getTopics().get("data-load-csv-inbound"));
    map.put(RESULT_VARIABLE, RESULT_VARIABLE);
    map.put(X_ACCESS_TOKEN, mockJwt());
    map.put(DIGITAL_SEAL, DIGITAL_SEAL);
    map.put(ENTITY_NAME, ENTITY_NAME);
    map.put(X_DIGITAL_SIGNATURE, X_DIGITAL_SIGNATURE);
    map.put(X_DIGITAL_SIGNATURE_DERIVED, X_DIGITAL_SIGNATURE_DERIVED);
    map.put(X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID, X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID);
    return new MessageHeaders(map);
  }
}
