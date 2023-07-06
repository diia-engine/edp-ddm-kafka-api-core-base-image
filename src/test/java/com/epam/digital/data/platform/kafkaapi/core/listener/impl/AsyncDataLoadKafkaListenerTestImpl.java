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

package com.epam.digital.data.platform.kafkaapi.core.listener.impl;

import com.epam.digital.data.platform.kafkaapi.core.audit.AuditableListener;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.UpsertCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.listener.AsyncDataLoadKafkaListener;
import com.epam.digital.data.platform.kafkaapi.core.service.CsvProcessor;
import com.epam.digital.data.platform.kafkaapi.core.util.Operation;
import java.util.Map;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;

@TestComponent
public class AsyncDataLoadKafkaListenerTestImpl extends AsyncDataLoadKafkaListener {

  protected AsyncDataLoadKafkaListenerTestImpl(Map<String, CsvProcessor> csvProcessorMap,
      Map<String, UpsertCommandHandler> commandHandlerMap) {
    super(csvProcessorMap, commandHandlerMap, Map.of());
  }

  @AuditableListener(Operation.CREATE)
  @Override
  @KafkaListener
  @SendTo
  public Message<String> asyncDataLoad(Message<String> input) {
    return super.asyncDataLoad(input);
  }
}
