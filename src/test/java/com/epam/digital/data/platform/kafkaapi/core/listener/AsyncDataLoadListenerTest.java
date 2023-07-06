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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.UpsertCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.listener.impl.AsyncDataLoadKafkaListenerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadRequest;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadResponse;
import com.epam.digital.data.platform.kafkaapi.core.service.FileService;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.kafkaapi.core.service.impl.CsvProcessorTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.EntityId;
import com.epam.digital.data.platform.model.core.kafka.File;
import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import com.epam.digital.data.platform.storage.file.dto.FileDataDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootTest(classes = {AsyncDataLoadKafkaListenerTestImpl.class})
@MockBeans({@MockBean(InputValidationService.class), @MockBean(ResponseMessageCreator.class),
        @MockBean(KafkaProperties.class), @MockBean(UpsertCommandHandlerTestImpl.class),
        @MockBean(PlatformTransactionManager.class)})
@Disabled
class AsyncDataLoadListenerTest {

    @SpyBean
    private ObjectMapper objectMapper;

    @MockBean
    private FileService fileService;
    @MockBean
    private KafkaTemplate<String, Message<String>> kafkaTemplate;
    @MockBean
    CsvProcessorTestImpl csvProcessor;
    @Mock
    Message<String> message;
    @Mock
    MessageHeaders headers;
    @Mock
    FileDataDto fileDataDto;
    @SpyBean
    private AsyncDataLoadKafkaListenerTestImpl listener;

    @Captor
    ArgumentCaptor<Message<String>> requestMessage;

    @BeforeEach
    void init() {
        when(message.getHeaders()).thenReturn(headers);
        String payload = getAsyncDataLoadRequest();
        when(message.getPayload()).thenReturn(payload);
        when(headers.get(any(), eq(String.class))).thenReturn("mock");
    }

    @Test
    @DisplayName("Check if status is CREATED if entities saved")
    void shouldCreateEntities() throws JsonProcessingException {
        when(fileService.retrieve(any(), any())).thenReturn(Optional.of(fileDataDto));
        when(csvProcessor.transformFileToEntities(fileDataDto)).thenReturn(List.of(new MockEntity()));
        var responseMessage = getResponseMessage(Status.SUCCESS, "OK");
//        when(listener.upsert(any())).thenReturn(responseMessage);
        var result = listener.asyncDataLoad(message);

        verify(kafkaTemplate).send(requestMessage.capture());
        var messageToKafka = requestMessage.getValue();
        var resultPayload = objectMapper.readValue(result.getPayload(), AsyncDataLoadResponse.class);
        assertThat(messageToKafka.getPayload()).isEqualTo(result.getPayload());
        assertThat(resultPayload.getStatus()).isEqualTo(Status.SUCCESS);
        assertThat(resultPayload.getDetails()).isEqualTo("OK");
    }

    @Test
    @DisplayName("Check if status is CONSTRAINT_VIOLATION if entities not saved")
    void shouldThrowConstraintViolationException() throws JsonProcessingException {
        when(fileService.retrieve(any(), any())).thenReturn(Optional.of(fileDataDto));
        when(csvProcessor.transformFileToEntities(fileDataDto)).thenReturn(List.of(new MockEntity()));
        var responseMessage = getResponseMessage(Status.CONSTRAINT_VIOLATION, "CONSTRAINT_VIOLATION");
//        when(listener.upsert(any())).thenReturn(responseMessage);
        var result = listener.asyncDataLoad(message);

        verify(kafkaTemplate).send(requestMessage.capture());
        var messageToKafka = requestMessage.getValue();
        var resultPayload = objectMapper.readValue(result.getPayload(), AsyncDataLoadResponse.class);
        assertThat(messageToKafka.getPayload()).isEqualTo(result.getPayload());
        assertThat(resultPayload.getStatus()).isEqualTo(Status.CONSTRAINT_VIOLATION);
        assertThat(resultPayload.getDetails()).isEqualTo("error: CONSTRAINT_VIOLATION in line: 1");
    }
    @Test
    @DisplayName("Check if status is OPERATION_FAILED if files not found in ceph bucket")
    void shouldThrowFileNotExistsException() throws JsonProcessingException {
        when(fileService.retrieve(any(), any())).thenReturn(Optional.empty());

        var result = listener.asyncDataLoad(message);

        verify(csvProcessor, never()).transformFileToEntities(fileDataDto);
//        verify(listener, never()).upsert(any());
        verify(kafkaTemplate).send(requestMessage.capture());
        var messageToKafka = requestMessage.getValue();
        var resultPayload = objectMapper.readValue(result.getPayload(), AsyncDataLoadResponse.class);
        assertThat(messageToKafka.getPayload()).isEqualTo(result.getPayload());
        assertThat(resultPayload.getStatus()).isEqualTo(Status.OPERATION_FAILED);
        assertThat(resultPayload.getDetails()).isEqualTo("Files not found in ceph bucket: [fileId, derivedFileId]");
    }

    @SneakyThrows
    private String getAsyncDataLoadRequest() {
        AsyncDataLoadRequest payload = new AsyncDataLoadRequest();
        File file = new File();
        file.setId("fileId");
        file.setChecksum("fileChecksum");
        File derivedFile = new File();
        derivedFile.setId("derivedFileId");
        derivedFile.setChecksum("derivedFileChecksum");
        payload.setFile(file);
        payload.setDerivedFile(derivedFile);
        return objectMapper.writeValueAsString(payload);
    }

    private Message<Response<EntityId>> getResponseMessage(Status status, String details) {
        Response<EntityId> payload = new Response<>();
        payload.setDetails(details);
        payload.setStatus(status);
        return MessageBuilder.withPayload(payload).build();
    }
}
