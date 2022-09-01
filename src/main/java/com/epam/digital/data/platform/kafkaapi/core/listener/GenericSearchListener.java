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

import com.epam.digital.data.platform.kafkaapi.core.exception.RequestProcessingException;
import com.epam.digital.data.platform.kafkaapi.core.searchhandler.AbstractSearchHandler;
import com.epam.digital.data.platform.kafkaapi.core.service.InputValidationService;
import com.epam.digital.data.platform.kafkaapi.core.service.ResponseMessageCreator;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Response;
import com.epam.digital.data.platform.model.core.kafka.Status;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;

public abstract class GenericSearchListener<I, O> {

  private static final String INPUT_IS_INVALID = "Input is invalid";

  private final Logger log = LoggerFactory.getLogger(GenericSearchListener.class);

  @Autowired
  private InputValidationService inputValidationService;
  @Autowired
  private ResponseMessageCreator responseMessageCreator;

  private final AbstractSearchHandler<I, O> searchHandler;

  protected GenericSearchListener(AbstractSearchHandler<I, O> searchHandler) {
    this.searchHandler = searchHandler;
  }

  public Message<Response<List<O>>> search(String key, Request<I> input) {
    Response<List<O>> response = new Response<>();

    try {
      var validationResult = inputValidationService.validate(key, input);
      if (!validationResult.isValid()) {
        log.info(INPUT_IS_INVALID);
        response.setStatus(validationResult.getStatus());
        return responseMessageCreator.createMessageByPayloadSize(response);
      }

      List<O> found = searchHandler.search(input);
      response.setPayload(found);
      response.setStatus(Status.SUCCESS);
    } catch (RequestProcessingException e) {
      log.error("Exception while request processing", e);
      response.setStatus(e.getKafkaResponseStatus());
      response.setDetails(e.getDetails());
    } catch (Exception e) {
      log.error("Unexpected exception while executing the 'delete' method", e);
      response.setStatus(Status.OPERATION_FAILED);
      response.setDetails("Unexpected exception while executing the 'delete' method");
    }

    return responseMessageCreator.createMessageByPayloadSize(response);
  }
}

