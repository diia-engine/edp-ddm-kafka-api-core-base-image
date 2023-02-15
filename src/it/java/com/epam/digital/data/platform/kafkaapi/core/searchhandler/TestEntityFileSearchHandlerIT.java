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

package com.epam.digital.data.platform.kafkaapi.core.searchhandler;

import static com.epam.digital.data.platform.kafkaapi.core.util.DaoTestUtils.TEST_ENTITY_FILE;
import static com.epam.digital.data.platform.kafkaapi.core.util.SearchHandlerTestUtil.mockRequest;

import com.epam.digital.data.platform.kafkaapi.core.config.TestConfiguration;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntityFile;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntityFileSearchConditions;
import com.epam.digital.data.platform.kafkaapi.core.impl.searchhandler.TestEntityFileSearchHandler;
import com.epam.digital.data.platform.model.core.kafka.Request;

import com.epam.digital.data.platform.model.core.search.SearchConditionPage;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@TestConfiguration
@SpringBootTest(classes = {TestEntityFileSearchHandler.class})
class TestEntityFileSearchHandlerIT {

  static final String STARTS_WITH = "FOP John";

  @Autowired
  TestEntityFileSearchHandler instance;

  TestEntityFileSearchConditions searchCriteria;
  Request<TestEntityFileSearchConditions> request;

  @BeforeEach
  void setup() {
    searchCriteria = new TestEntityFileSearchConditions();
    request = mockRequest(searchCriteria);
  }

  @Test
  void shouldFindAllWhenEmptySearchCriteria() {
    final SearchConditionPage<TestEntityFile> allRecords = instance.search(request);
    Assertions.assertThat(allRecords.getContent()).hasSize(2);
  }

  @Test
  void shouldSearchByMultipleSearchCriteria() {
    searchCriteria.setLegalEntityName(STARTS_WITH);

    final SearchConditionPage<TestEntityFile> found = instance.search(request);

    Assertions.assertThat(found.getContent()).hasSize(1);
    Assertions.assertThat(found.getContent().get(0).getLegalEntityName())
        .isEqualTo(TEST_ENTITY_FILE.getLegalEntityName());
    Assertions.assertThat(found.getContent().get(0).getScanCopy().getId())
        .isEqualTo(TEST_ENTITY_FILE.getScanCopy().getId());
    Assertions.assertThat(found.getContent().get(0).getScanCopy().getChecksum())
        .isEqualTo(TEST_ENTITY_FILE.getScanCopy().getChecksum());
  }
}
