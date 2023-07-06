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

import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.GENERIC_REQUEST_PROCESSING_EXCEPTION_MESSAGE;
import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.UNEXPECTED_EXCEPTION_MESSAGE_FORMAT;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_ACCESS_TOKEN;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_DIGITAL_SIGNATURE;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_DIGITAL_SIGNATURE_DERIVED;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_APPLICATION;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_BUSINESS_ACTIVITY;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_BUSINESS_ACTIVITY_INSTANCE_ID;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_BUSINESS_PROCESS;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_BUSINESS_PROCESS_DEFINITION_ID;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID;
import static com.epam.digital.data.platform.kafkaapi.core.util.Header.X_SOURCE_SYSTEM;
import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.UpsertCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.exception.ConstraintViolationException;
import com.epam.digital.data.platform.kafkaapi.core.exception.FileNotExistsException;
import com.epam.digital.data.platform.kafkaapi.core.exception.RequestProcessingException;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadRequest;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadResponse;
import com.epam.digital.data.platform.kafkaapi.core.model.AsyncDataLoadResult;
import com.epam.digital.data.platform.kafkaapi.core.service.CsvProcessor;
import com.epam.digital.data.platform.kafkaapi.core.service.FileService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.model.core.kafka.EntityId;
import com.epam.digital.data.platform.model.core.kafka.File;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.RequestContext;
import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.SecurityContext;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.epam.digital.data.platform.starter.kafka.config.properties.KafkaProperties;
import com.epam.digital.data.platform.storage.file.dto.FileDataDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.CaseUtils;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;


public abstract class AsyncDataLoadKafkaListener {

  private final Logger log = LoggerFactory.getLogger(AsyncDataLoadKafkaListener.class);

  private static final String OUTBOUND_TOPIC = "data-load-csv-outbound";
  private static final String ENTITY_NAME = "EntityName";
  private static final String RESULT_VARIABLE = "ResultVariable";

  @Autowired
  PlatformTransactionManager transactionManager;
  @Autowired
  private FileService fileService;
  @Autowired
  private KafkaProperties kafkaProperties;
  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private KafkaTemplate<String, Message<String>> kafkaTemplate;
  @Autowired
  private ResponseMessageCreator responseMessageCreator;
  private final Map<String, CsvProcessor> csvProcessorMap;
  private final Map<String, UpsertCommandHandler> commandHandlerMap;
  private final Map<String, String> entityNamesToSchemaNames;

  protected AsyncDataLoadKafkaListener(Map<String, CsvProcessor> csvProcessorMap,
      Map<String, UpsertCommandHandler> commandHandlerMap,
      Map<String, String> entityNamesToSchemaNames) {
    this.csvProcessorMap = csvProcessorMap;
    this.commandHandlerMap = commandHandlerMap;
    this.entityNamesToSchemaNames = entityNamesToSchemaNames;
  }

  public Message<String> asyncDataLoad(Message<String> requestMessage) {
    var schemaName = getSchemaName(requestMessage);
    var csvProcessor = csvProcessorMap.get(schemaName + "AsyncDataLoadCsvProcessor");
    var commandHandler = commandHandlerMap.get(schemaName + "UpsertCommandHandler");

    TransactionDefinition def = new DefaultTransactionDefinition();
    TransactionStatus transactionStatus = transactionManager.getTransaction(def);

    MessageHeaders requestMessageHeaders = requestMessage.getHeaders();
    RequestContext requestContext = buildRequestContext(requestMessageHeaders);
    SecurityContext securityContext = buildSecurityContext(requestMessageHeaders);

    AsyncDataLoadResponse loadResponse = new AsyncDataLoadResponse();
    loadResponse.setRequestContext(requestContext);

    List<Object> list;
    Status status = null;
    String details = "";
    try {
      AsyncDataLoadRequest payload = objectMapper.readValue(requestMessage.getPayload(),
          AsyncDataLoadRequest.class);
      FileDataDto fileDataDto = getFileDataDto(payload,
          requestContext.getBusinessProcessInstanceId());
      list = csvProcessor.transformFileToEntities(fileDataDto);

      for (int i = 0; i < list.size(); i++) {
        var request = new Request<>(list.get(i), requestContext, securityContext);
        Response<EntityId> response = (Response<EntityId>) upsert(commandHandler,
            request).getPayload();

        if (!response.getStatus().equals(Status.SUCCESS)) {
          var message = String.format("error: %s in line: %d", response.getDetails(), (i + 2));
          throw new ConstraintViolationException(message, details);
        }

        details = "OK";
        status = Status.SUCCESS;
      }

      transactionManager.commit(transactionStatus);
    } catch (ConstraintViolationException e) {
      status = e.getKafkaResponseStatus();
      details = e.getMessage();
      transactionManager.rollback(transactionStatus);
    } catch (Exception e) {
      details = e.getMessage();
      status = Status.OPERATION_FAILED;
      transactionManager.rollback(transactionStatus);
    }

    loadResponse.setStatus(status);
    loadResponse.setDetails(details);
    String resultVariable = requestMessageHeaders.get(RESULT_VARIABLE, String.class);
    String entityName = requestMessageHeaders.get(ENTITY_NAME, String.class);
    AsyncDataLoadResult asyncDataLoadResult = buildAsyncDataLoadResult(resultVariable, entityName);
    loadResponse.setPayload(asyncDataLoadResult);
    String convertedPayload;
    try {
      convertedPayload = objectMapper.writeValueAsString(loadResponse);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unexpected serialization error", e);
    }
    MessageHeaders messageHeaders = getMessageHeaders(requestMessageHeaders);
    Message<String> responseMessage = MessageBuilder.withPayload(convertedPayload)
        .copyHeaders(messageHeaders).build();

    kafkaTemplate.send(responseMessage);

    return responseMessage;
  }

  private String getSchemaName(Message<String> requestMessage) {
    var entityName = requestMessage.getHeaders().get("EntityName").toString();
    var entityNameCamelCase = CaseUtils.toCamelCase(entityName, true, '-', '_');
    return StringUtils.uncapitalize(entityNamesToSchemaNames.get(entityNameCamelCase));
  }

  private Message<Response<EntityId>> upsert(UpsertCommandHandler<Object> commandHandler,
      Request<Object> input) {
    Response<EntityId> response = new Response<>();

    try {
      response.setPayload(commandHandler.upsert(input));
      response.setStatus(Status.SUCCESS);
    } catch (RequestProcessingException e) {
      log.error(GENERIC_REQUEST_PROCESSING_EXCEPTION_MESSAGE, e.getMessage(), e);
      response.setStatus(e.getKafkaResponseStatus());
      response.setDetails(e.getDetails());
    } catch (Exception e) {
      var exceptionMessage = String.format(UNEXPECTED_EXCEPTION_MESSAGE_FORMAT, "upsert",
          e.getMessage());
      log.error(exceptionMessage, e);
      response.setStatus(Status.OPERATION_FAILED);
      response.setDetails(exceptionMessage);
    }

    return responseMessageCreator.createMessageByPayloadSize(response);
  }

  private MessageHeaders getMessageHeaders(MessageHeaders requestMessageHeaders) {
    Map<String, Object> map = new HashMap<>(requestMessageHeaders);
    map.put(TOPIC, kafkaProperties.getTopics().get(OUTBOUND_TOPIC));
    return new MessageHeaders(map);
  }

  private SecurityContext buildSecurityContext(MessageHeaders headers) {
    SecurityContext context = new SecurityContext();
    context.setAccessToken(getFromHeaders(headers, X_ACCESS_TOKEN));
    String digitalSignature = getFromHeaders(headers, X_DIGITAL_SIGNATURE);
    context.setDigitalSignature(digitalSignature);
    context.setDigitalSignatureChecksum(DigestUtils.sha256Hex(digitalSignature));
    String digitalSignatureDerived = getFromHeaders(headers, X_DIGITAL_SIGNATURE_DERIVED);
    context.setDigitalSignatureDerived(digitalSignatureDerived);
    context.setDigitalSignatureDerivedChecksum(DigestUtils.sha256Hex(digitalSignatureDerived));
    return context;
  }

  private RequestContext buildRequestContext(MessageHeaders headers) {
    RequestContext context = new RequestContext();
    context.setApplication(getFromHeaders(headers, X_SOURCE_APPLICATION));
    context.setSystem(getFromHeaders(headers, X_SOURCE_SYSTEM));
    context.setBusinessProcess(getFromHeaders(headers, X_SOURCE_BUSINESS_PROCESS));
    context.setBusinessProcessInstanceId(
        getFromHeaders(headers, X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID));
    context.setBusinessActivity(getFromHeaders(headers, X_SOURCE_BUSINESS_ACTIVITY));
    context.setBusinessActivityInstanceId(
        getFromHeaders(headers, X_SOURCE_BUSINESS_ACTIVITY_INSTANCE_ID));
    context.setBusinessProcessDefinitionId(
        getFromHeaders(headers, X_SOURCE_BUSINESS_PROCESS_DEFINITION_ID));
    return context;
  }

  private static String getFromHeaders(MessageHeaders headers, String header) {
    return headers.get(WordUtils.capitalizeFully(header, '-'), String.class);
  }

  private AsyncDataLoadResult buildAsyncDataLoadResult(String resultVariable, String entityName) {
    AsyncDataLoadResult asyncDataLoadResult = new AsyncDataLoadResult();
    asyncDataLoadResult.setResultVariable(resultVariable);
    asyncDataLoadResult.setEntityName(entityName);
    return asyncDataLoadResult;
  }

  private FileDataDto getFileDataDto(AsyncDataLoadRequest payload, String instanceId) {
    File derivedFile = payload.getDerivedFile();
    File file = payload.getFile();
    Optional<FileDataDto> fileDataDto = fileService.retrieve(instanceId, derivedFile);
    fileDataDto = fileDataDto.isEmpty() ? fileService.retrieve(instanceId, file) : fileDataDto;
    if (fileDataDto.isEmpty()) {
      throw new FileNotExistsException("Files not found in ceph bucket",
          List.of(file.getId(), derivedFile.getId()));
    }
    return fileDataDto.get();
  }

}
