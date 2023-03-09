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

import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.GENERIC_REQUEST_PROCESSING_EXCEPTION_MESSAGE;
import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.INPUT_IS_INVALID_MESSAGE;
import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.UNEXPECTED_EXCEPTION_MESSAGE_FORMAT;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.UpdateCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.exception.RequestProcessingException;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;

public abstract class GenericUpdateCommandListener<O> {

  private final Logger log = LoggerFactory.getLogger(GenericUpdateCommandListener.class);

  @Autowired
  private InputValidationService inputValidationService;
  @Autowired
  private ResponseMessageCreator responseMessageCreator;

  private final UpdateCommandHandler<O> commandHandler;

  protected GenericUpdateCommandListener(UpdateCommandHandler<O> commandHandler) {
    this.commandHandler = commandHandler;
  }

  public Message<Response<Void>> update(String key, Request<O> input) {
    Response<Void> response = new Response<>();

    try {
      var validationResult = inputValidationService.validate(key, input);
      if (!validationResult.isValid()) {
        log.warn(INPUT_IS_INVALID_MESSAGE, validationResult.getStatus());
        response.setStatus(validationResult.getStatus());
        return responseMessageCreator.createMessageByPayloadSize(response);
      }

      commandHandler.update(input);
      response.setStatus(Status.NO_CONTENT);
    } catch (RequestProcessingException e) {
      log.error(GENERIC_REQUEST_PROCESSING_EXCEPTION_MESSAGE, e.getMessage(), e);
      response.setStatus(e.getKafkaResponseStatus());
      response.setDetails(e.getDetails());
    } catch (Exception e) {
      var exceptionMessage = String.format(UNEXPECTED_EXCEPTION_MESSAGE_FORMAT, "update",
          e.getMessage());
      log.error(exceptionMessage, e);
      response.setStatus(Status.OPERATION_FAILED);
      response.setDetails(exceptionMessage);
    }

    return responseMessageCreator.createMessageByPayloadSize(response);
  }
}
