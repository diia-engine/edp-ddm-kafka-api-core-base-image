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

package com.epam.digital.data.platform.kafkaapi.core.audit;

import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.BUSINESS_ACTIVITY;
import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.BUSINESS_ACTIVITY_INSTANCE_ID;
import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.BUSINESS_PROCESS;
import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.BUSINESS_PROCESS_DEFINITION_ID;
import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.BUSINESS_PROCESS_INSTANCE_ID;
import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.SOURCE_APPLICATION;
import static com.epam.digital.data.platform.kafkaapi.core.audit.AuditEventUtils.SOURCE_SYSTEM;
import static org.assertj.core.api.Assertions.assertThat;

import com.epam.digital.data.platform.kafkaapi.core.util.Header;
import com.epam.digital.data.platform.starter.audit.model.AuditSourceInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class MdcAuditSourceInfoProviderTest {

  private final AuditSourceInfoProvider auditSourceInfoProvider = new MdcAuditSourceInfoProvider();

  @Test
  void expectCorrectAuditSourceInfoRetrievedFromMdc() {
    MDC.put(Header.X_SOURCE_SYSTEM, SOURCE_SYSTEM);
    MDC.put(Header.X_SOURCE_APPLICATION, SOURCE_APPLICATION);
    MDC.put(Header.X_SOURCE_BUSINESS_PROCESS, BUSINESS_PROCESS);
    MDC.put(Header.X_SOURCE_BUSINESS_PROCESS_DEFINITION_ID, BUSINESS_PROCESS_DEFINITION_ID);
    MDC.put(Header.X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID, BUSINESS_PROCESS_INSTANCE_ID);
    MDC.put(Header.X_SOURCE_BUSINESS_ACTIVITY, BUSINESS_ACTIVITY);
    MDC.put(Header.X_SOURCE_BUSINESS_ACTIVITY_INSTANCE_ID, BUSINESS_ACTIVITY_INSTANCE_ID);

    var actualSourceInfo = auditSourceInfoProvider.getAuditSourceInfo();

    var expectedSourceInfo =
        AuditSourceInfo.AuditSourceInfoBuilder.anAuditSourceInfo()
            .system(SOURCE_SYSTEM)
            .application(SOURCE_APPLICATION)
            .businessProcess(BUSINESS_PROCESS)
            .businessProcessDefinitionId(BUSINESS_PROCESS_DEFINITION_ID)
            .businessProcessInstanceId(BUSINESS_PROCESS_INSTANCE_ID)
            .businessActivity(BUSINESS_ACTIVITY)
            .businessActivityInstanceId(BUSINESS_ACTIVITY_INSTANCE_ID)
            .build();

    assertThat(actualSourceInfo).usingRecursiveComparison().isEqualTo(expectedSourceInfo);
  }

  @AfterEach
  void afterEach() {
    MDC.clear();
  }
}
