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

import static com.epam.digital.data.platform.kafkaapi.core.util.DaoTestUtils.TEST_ENTITY;
import static com.epam.digital.data.platform.kafkaapi.core.util.SearchHandlerTestUtil.mockRequest;

import com.epam.digital.data.platform.kafkaapi.core.config.TestConfiguration;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntity;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntitySearchConditions;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TypGender;
import com.epam.digital.data.platform.kafkaapi.core.impl.searchhandler.TestEntitySearchHandler;
import com.epam.digital.data.platform.model.core.kafka.Request;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@TestConfiguration
@SpringBootTest(classes = {TestEntitySearchHandler.class})
class SearchHandlerIT {

  static final String STARTS_WITH = "John";

  @Autowired
  TestEntitySearchHandler instance;

  TestEntitySearchConditions searchCriteria;
  Request<TestEntitySearchConditions> request;

  @BeforeEach
  void setup() {
    searchCriteria = new TestEntitySearchConditions();
    request = mockRequest(searchCriteria);
  }

  @Test
  void shouldFindAllWhenEmptySearchCriteria() {
    final List<TestEntity> allRecords = instance.search(request);
    Assertions.assertThat(allRecords).hasSize(3);
  }

  @Test
  void shouldSearchByMultipleSearchCriteria() {
    searchCriteria.setPersonFullName(STARTS_WITH);
    searchCriteria.setPersonGender(TypGender.M);

    final List<TestEntity> found = instance.search(request);

    Assertions.assertThat(found).hasSize(2);
    Assertions.assertThat(found.get(0).getPersonFullName())
        .isEqualTo(TEST_ENTITY.getPersonFullName());
    Assertions.assertThat(found.get(0).getPersonGender()).isEqualTo(TEST_ENTITY.getPersonGender());
  }
}
