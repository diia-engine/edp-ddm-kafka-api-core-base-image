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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl.DeleteCommandHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.model.DmlOperationArgs;
import com.epam.digital.data.platform.kafkaapi.core.tabledata.MockEntityTableDataProviderImpl;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.DmlOperationHandler;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.service.JwtInfoProvider;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.RequestContext;
import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest(classes = DeleteCommandHandlerTestImpl.class)
class AbstractDeleteCommandHandlerTest {

  private static final String USER_ID = "user";

  private static final String TABLE_NAME = "table";
  private static final String PK_COLUMN_NAME = "consent_id";

  private static final UUID ENTITY_ID = UUID.fromString("123e4567-e89b-12d3-a456-426655440000");
  private final Request<MockEntity> request =
      new Request<>(getMockedFactor(), new RequestContext(), null);
  private final JwtClaimsDto userClaims = getMockedClaims();
  @MockBean
  private EntityConverter<MockEntity> entityConverter;
  @MockBean
  private MockEntityTableDataProviderImpl tableDataProvider;
  @MockBean
  private DmlOperationHandler dmlOperationHandler;
  @MockBean
  private JwtInfoProvider jwtInfoProvider;
  @Autowired
  private DeleteCommandHandlerTestImpl commandHandler;

  @BeforeEach
  void setUp() {

    when(jwtInfoProvider.getUserClaims(request)).thenReturn(userClaims);
    when(tableDataProvider.tableName()).thenReturn(TABLE_NAME);
    when(tableDataProvider.pkColumnName()).thenReturn(PK_COLUMN_NAME);
  }

  @Test
  void expectDeleteOperationWithPreparedParamsCalled() {
    when(entityConverter.getUuidOfEntity(request.getPayload(), PK_COLUMN_NAME))
        .thenReturn(ENTITY_ID.toString());
    Map<String, String> mockSysValuesMap = new HashMap<>();
    when(entityConverter.buildSysValues(USER_ID, request)).thenReturn(mockSysValuesMap);

    commandHandler.delete(request);

    verify(entityConverter).getUuidOfEntity(request.getPayload(), PK_COLUMN_NAME);
    verify(entityConverter).buildSysValues(USER_ID, request);
    verify(dmlOperationHandler)
        .delete(
            DmlOperationArgs.builder(TABLE_NAME, userClaims, mockSysValuesMap)
                .deleteOperationArgs(ENTITY_ID.toString())
                .build());
  }

  private JwtClaimsDto getMockedClaims() {
    JwtClaimsDto userClaims = new JwtClaimsDto();
    userClaims.setDrfo(USER_ID);
    return userClaims;
  }

  private MockEntity getMockedFactor() {
    MockEntity dto = new MockEntity();
    dto.setConsentId(ENTITY_ID);
    dto.setPersonFullName("name");
    return dto;
  }
}
