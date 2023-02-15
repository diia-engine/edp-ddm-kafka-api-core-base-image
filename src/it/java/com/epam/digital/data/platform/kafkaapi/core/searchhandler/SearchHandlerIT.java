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

import com.epam.digital.data.platform.kafkaapi.core.config.TestConfiguration;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.PagingTestEntitySearchConditions;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntity;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntitySearchConditions;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TypGender;
import com.epam.digital.data.platform.kafkaapi.core.impl.searchhandler.PagingTestEntitySearchHandler;
import com.epam.digital.data.platform.kafkaapi.core.impl.searchhandler.TestEntitySearchHandler;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.search.SearchConditionPage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static com.epam.digital.data.platform.kafkaapi.core.util.DaoTestUtils.TEST_ENTITY;
import static com.epam.digital.data.platform.kafkaapi.core.util.SearchHandlerTestUtil.mockRequest;
import static org.assertj.core.api.Assertions.assertThat;

@TestConfiguration
@SpringBootTest(classes = {TestEntitySearchHandler.class, PagingTestEntitySearchHandler.class})
class SearchHandlerIT {

  static final String STARTS_WITH = "John";

  @Autowired
  TestEntitySearchHandler instance;
  @Autowired
  PagingTestEntitySearchHandler pagingInstance;

  TestEntitySearchConditions searchCriteria;
  PagingTestEntitySearchConditions pagingSearchCriteria;
  Request<TestEntitySearchConditions> request;
  Request<PagingTestEntitySearchConditions> pagingRequest;

  @BeforeEach
  void setup() {
    searchCriteria = new TestEntitySearchConditions();
    request = mockRequest(searchCriteria);

    pagingSearchCriteria = new PagingTestEntitySearchConditions();
    pagingRequest = mockRequest(pagingSearchCriteria);
  }

  @Test
  void shouldFindAllWhenEmptySearchCriteria() {
    final SearchConditionPage<TestEntity> allRecords = instance.search(request);
    assertThat(allRecords.getContent()).hasSize(3);
  }

  @Test
  void shouldSearchByMultipleSearchCriteria() {
    searchCriteria.setPersonFullName(STARTS_WITH);
    searchCriteria.setPersonGender(TypGender.M);

    final SearchConditionPage<TestEntity> found = instance.search(request);

    assertThat(found.getContent()).hasSize(2);
    assertThat(found.getContent().get(0).getPersonFullName())
        .isEqualTo(TEST_ENTITY.getPersonFullName());
    assertThat(found.getContent().get(0).getPersonGender()).isEqualTo(TEST_ENTITY.getPersonGender());
    assertThat(found.getTotalElements()).isNull();
    assertThat(found.getTotalPages()).isNull();
    assertThat(found.getPageNo()).isNull();
    assertThat(found.getPageSize()).isNull();
  }

  @Test
  void shouldFindPagedResponseByMultipleSearchCriteria() {
    pagingSearchCriteria.setPersonFullName(STARTS_WITH);
    pagingSearchCriteria.setPersonGender(TypGender.M);
    pagingSearchCriteria.setPageNo(1);
    pagingSearchCriteria.setPageSize(1);

    final SearchConditionPage<TestEntity> found = pagingInstance.search(pagingRequest);

    assertThat(found.getContent()).hasSize(1);
    assertThat(found.getContent().get(0).getPersonFullName()).isEqualTo(TEST_ENTITY.getPersonFullName());
    assertThat(found.getContent().get(0).getPersonGender()).isEqualTo(TEST_ENTITY.getPersonGender());
    assertThat(found.getPageNo()).isEqualTo(1);
    assertThat(found.getPageSize()).isEqualTo(1);
    assertThat(found.getTotalPages()).isEqualTo(2);
    assertThat(found.getTotalElements()).isEqualTo(2);
  }
}
