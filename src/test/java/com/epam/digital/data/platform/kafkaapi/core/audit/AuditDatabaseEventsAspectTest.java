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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.aspect.AuditAspect;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.model.DmlOperationArgs;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.DmlOperationHandler;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.config.JooqTestConfig;
import com.epam.digital.data.platform.kafkaapi.core.exception.ForbiddenOperationException;
import com.epam.digital.data.platform.kafkaapi.core.exception.ProcedureErrorException;
import com.epam.digital.data.platform.kafkaapi.core.exception.RequestProcessingException;
import com.epam.digital.data.platform.kafkaapi.core.exception.SqlErrorException;
import com.epam.digital.data.platform.kafkaapi.core.queryhandler.impl.QueryHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.searchhandler.impl.AbstractSearchHandlerTestImpl;
import com.epam.digital.data.platform.kafkaapi.core.service.AccessPermissionService;
import com.epam.digital.data.platform.kafkaapi.core.service.JwtInfoProvider;
import com.epam.digital.data.platform.kafkaapi.core.tabledata.MockEntityTableDataProviderImpl;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.SecurityContext;
import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;
import com.epam.digital.data.platform.starter.security.dto.RolesDto;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

@Import({AopAutoConfiguration.class})
@SpringBootTest(
    classes = {
        AuditAspect.class,
        DatabaseAuditProcessor.class,
        DmlOperationHandler.class,
        QueryHandlerTestImpl.class,
        AbstractSearchHandlerTestImpl.class,
    })
@MockBean(KafkaAuditProcessor.class)
@MockBean(JwtInfoProvider.class)
@MockBean(EntityConverter.class)
@ContextConfiguration(classes = JooqTestConfig.class)
class AuditDatabaseEventsAspectTest {

  private static final String ENTITY_ID = "123e4567-e89b-12d3-a456-426655440000";
  private static String ACCESS_TOKEN;

  @Autowired
  private DmlOperationHandler dmlOperationHandler;
  @Autowired
  private QueryHandlerTestImpl abstractQueryHandler;
  @Autowired
  private AbstractSearchHandlerTestImpl abstractSearchHandlerTest;

  @MockBean
  private AccessPermissionService accessPermissionService;
  @MockBean
  private DatabaseEventsFacade databaseEventsFacade;
  @MockBean
  private DataSource dataSource;
  @MockBean
  private MockEntityTableDataProviderImpl tableDataProvider;

  @Mock
  private ResultSet resultSet;
  @Mock
  private Connection connection;
  @Mock
  private CallableStatement callableStatement;

  private DmlOperationArgs mockSaveArgs;
  private DmlOperationArgs mockUpdateArgs;
  private DmlOperationArgs mockDeleteArgs;

  @BeforeAll
  static void init() throws IOException {
    ACCESS_TOKEN = new String(AuditDatabaseEventsAspectTest.class
        .getResourceAsStream("/accessToken.json").readAllBytes());
  }

  @BeforeEach
  void beforeEach() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareCall(any())).thenReturn(callableStatement);
    when(callableStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString("f_row_insert")).thenReturn("42");
    mockSaveArgs = DmlOperationArgs.builder("", mockJwtClaimsDto(),
        new HashMap<>()).saveOperationArgs(new HashMap<>()).build();
    mockUpdateArgs = DmlOperationArgs.builder("", mockJwtClaimsDto(),
        new HashMap<>()).updateOperationArgs(ENTITY_ID, new HashMap<>()).build();
    mockDeleteArgs = DmlOperationArgs.builder("", mockJwtClaimsDto(),
        new HashMap<>()).deleteOperationArgs(ENTITY_ID).build();
  }

  @Test
  void expectAuditAspectBeforeAndAfterFindByIdMethodWhenNoException() {
    when(accessPermissionService.hasReadAccess(any(), any())).thenReturn(true);
    when(tableDataProvider.tableName()).thenReturn("table");
    when(tableDataProvider.pkColumnName()).thenReturn("consent_id");

    abstractQueryHandler.findById(mockRequest(ACCESS_TOKEN, ENTITY_ID));

    verify(databaseEventsFacade, times(2))
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectOnlyBeforeWhenExceptionOnFindByIdMethod() {
    assertThrows(
        ForbiddenOperationException.class,
        () -> abstractQueryHandler.findById(mockRequest(ACCESS_TOKEN, ENTITY_ID)));

    verify(databaseEventsFacade)
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterSaveMethodWhenNoExceptionAndResultExist() {

    dmlOperationHandler.save(mockSaveArgs);

    verify(databaseEventsFacade, times(2))
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectOnlyBeforeWhenExceptionOnSaveMethod() throws SQLException {
    when(callableStatement.executeQuery()).thenThrow(new SQLException());

    assertThrows(
        RequestProcessingException.class,
        () -> dmlOperationHandler.save(mockSaveArgs));

    verify(databaseEventsFacade)
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectOnlyBeforeWhenResultDoesNotExist() throws SQLException {
    when(resultSet.next()).thenReturn(false);

    assertThrows(
        ProcedureErrorException.class,
        () -> dmlOperationHandler.save(mockSaveArgs));

    verify(databaseEventsFacade)
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterUpdateMethodWhenNoException() {

    dmlOperationHandler.update(mockUpdateArgs);

    verify(databaseEventsFacade, times(2))
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectOnlyBeforeWhenExceptionOnUpdateMethod() throws SQLException {
    when(callableStatement.execute()).thenThrow(new SQLException());

    assertThrows(
        RequestProcessingException.class,
        () -> dmlOperationHandler
            .update(mockSaveArgs));

    verify(databaseEventsFacade)
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectBeforeAndAfterDeleteMethodWhenNoException() {

    dmlOperationHandler.delete(mockDeleteArgs);

    verify(databaseEventsFacade, times(2))
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  void expectAuditAspectOnlyBeforeWhenExceptionOnDeleteMethod() throws SQLException {
    when(callableStatement.execute()).thenThrow(new SQLException());

    assertThrows(
        RequestProcessingException.class,
        () -> dmlOperationHandler.delete(mockDeleteArgs));

    verify(databaseEventsFacade)
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  @DirtiesContext
  void expectAuditAspectBeforeAndAfterSearchMethodWhenNoException() {
    abstractSearchHandlerTest.search(mockRequest(null, null));

    verify(databaseEventsFacade, times(2))
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  @DirtiesContext
  void expectAuditAspectOnlyBeforeWhenExceptionOnSearchMethod() {
    abstractSearchHandlerTest.setTableName(null);
    var input = mockRequest(null, null);

    assertThrows(
        SqlErrorException.class, () -> abstractSearchHandlerTest.search(input));

    verify(databaseEventsFacade)
        .sendDbAudit(any(), any(), any(), any(), any(), any(), any(), any());
  }

  private JwtClaimsDto mockJwtClaimsDto() {
    JwtClaimsDto result = new JwtClaimsDto();
    result.setRoles(List.of("user", "manager"));

    RolesDto realmAccess = new RolesDto();
    realmAccess.setRoles(List.of("admin"));
    result.setRealmAccess(realmAccess);

    result.setDrfo("1010101014");
    result.setFullName("Сидоренко Василь Леонідович");
    return result;
  }

  private <T> Request mockRequest(String jwt, T payload) {
    SecurityContext sc = new SecurityContext(jwt, null, null);
    return new Request(payload, null, sc);
  }
}