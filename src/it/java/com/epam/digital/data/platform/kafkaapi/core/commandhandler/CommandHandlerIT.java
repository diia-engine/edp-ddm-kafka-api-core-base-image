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

package com.epam.digital.data.platform.kafkaapi.core.commandhandler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.DmlOperationHandler;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.config.TestConfiguration;
import com.epam.digital.data.platform.kafkaapi.core.impl.commandhandler.TestEntityCreateCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.impl.commandhandler.TestEntityDeleteCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.impl.commandhandler.TestEntityUpdateCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntity;
import com.epam.digital.data.platform.kafkaapi.core.impl.tabledata.TestEntityTableDataProvider;
import com.epam.digital.data.platform.kafkaapi.core.service.JwtInfoProvider;
import com.epam.digital.data.platform.kafkaapi.core.util.DaoTestUtils;
import com.epam.digital.data.platform.kafkaapi.core.util.SecurityUtils;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.RequestContext;
import com.epam.digital.data.platform.starter.security.jwt.TokenParser;
import com.nimbusds.jose.JOSEException;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@TestConfiguration
@SpringBootTest(
    classes = {
        TestEntityCreateCommandHandler.class,
        TestEntityUpdateCommandHandler.class,
        TestEntityDeleteCommandHandler.class,
        EntityConverter.class,
        TestEntityTableDataProvider.class,
        DmlOperationHandler.class,
        JwtInfoProvider.class,
        TokenParser.class
    })
class CommandHandlerIT {

  @Autowired
  TestEntityCreateCommandHandler createCommandHandler;
  @Autowired
  TestEntityUpdateCommandHandler updateCommandHandler;
  @Autowired
  TestEntityDeleteCommandHandler deleteCommandHandler;

  TestEntity newTestRecord;
  Request<TestEntity> newTestRequest;
  TestEntity existingTestRecord;
  Request<TestEntity> existingTestRequest;

  @BeforeEach
  void setUp() throws JOSEException {
    newTestRecord = new TestEntity();
    newTestRecord.setConsentDate(LocalDateTime.now());
    newTestRecord.setPersonFullName("Kurylo Volodymyr");

    existingTestRecord = DaoTestUtils.testEntity();

    newTestRequest = new Request<>(newTestRecord, new RequestContext(),
        SecurityUtils.mockSecurityContext());
    existingTestRequest = new Request<>(existingTestRecord, new RequestContext(),
        SecurityUtils.mockSecurityContext());
  }

  @Test
  @DisplayName("Save new record has no errors on processing")
  void expectNoErrorsWhenSave() {
    Assertions.assertDoesNotThrow(() -> createCommandHandler.save(newTestRequest));
  }

  @Test
  @DisplayName("Update record has no errors on processing")
  void expectNoErrorsWhenUpdate() {
    String updatedName = "Test User";
    existingTestRecord.setPersonFullName(updatedName);

    assertDoesNotThrow(() -> updateCommandHandler.update(existingTestRequest));
  }

  @Test
  @DisplayName("Delete record has no errors on processing")
  void expectNoErrorsWhenDelete() {
    assertDoesNotThrow(() -> deleteCommandHandler.delete(existingTestRequest));
  }
}
