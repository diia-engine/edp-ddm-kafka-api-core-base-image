package com.epam.digital.data.platform.kafkaapi.core.listener;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.DeleteCommandHandler;
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

import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.GENERIC_REQUEST_PROCESSING_EXCEPTION_MESSAGE;
import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.INPUT_IS_INVALID_MESSAGE;
import static com.epam.digital.data.platform.kafkaapi.core.util.ExceptionMessage.UNEXPECTED_EXCEPTION_MESSAGE_FORMAT;

public abstract class GenericDeleteCommandListener<O> {

  private final Logger log = LoggerFactory.getLogger(GenericDeleteCommandListener.class);

  @Autowired
  private InputValidationService inputValidationService;
  @Autowired
  private ResponseMessageCreator responseMessageCreator;

  private final DeleteCommandHandler<O> commandHandler;

  protected GenericDeleteCommandListener(DeleteCommandHandler<O> commandHandler) {
    this.commandHandler = commandHandler;
  }

  public Message<Response<Void>> delete(String key, Request<O> input) {
    Response<Void> response = new Response<>();

    try {
      var validationResult = inputValidationService.validate(key, input);
      if (!validationResult.isValid()) {
        log.info(INPUT_IS_INVALID_MESSAGE);
        response.setStatus(validationResult.getStatus());
        return responseMessageCreator.createMessageByPayloadSize(response);
      }

      commandHandler.delete(input);
      response.setStatus(Status.NO_CONTENT);
    } catch (RequestProcessingException e) {
      log.error(GENERIC_REQUEST_PROCESSING_EXCEPTION_MESSAGE, e);
      response.setStatus(e.getKafkaResponseStatus());
      response.setDetails(e.getDetails());
    } catch (Exception e) {
      var exceptionMessage = String.format(UNEXPECTED_EXCEPTION_MESSAGE_FORMAT, "delete");
      log.error(exceptionMessage, e);
      response.setStatus(Status.OPERATION_FAILED);
      response.setDetails(exceptionMessage);
    }

    return responseMessageCreator.createMessageByPayloadSize(response);
  }
}
