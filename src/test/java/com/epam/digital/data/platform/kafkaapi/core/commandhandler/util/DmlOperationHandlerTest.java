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

package com.epam.digital.data.platform.kafkaapi.core.commandhandler.util;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.model.DmlOperationArgs;
import com.epam.digital.data.platform.kafkaapi.core.exception.ConstraintViolationException;
import com.epam.digital.data.platform.kafkaapi.core.exception.ForbiddenOperationException;
import com.epam.digital.data.platform.kafkaapi.core.exception.ProcedureErrorException;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;
import com.epam.digital.data.platform.starter.security.dto.RolesDto;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DmlOperationHandlerTest {

  private static final String TABLE_NAME = "table";
  private static final List<String> ROLES = singletonList("role");
  private static final String ENTITY_ID = "1";
  private static final String SYS_VALUES_HSTORE_FORMATTED =
      "\"curr_user\"=>\"2\", \"source_system\"=>\"system\"";
  private static final String BUSINESS_VALUES_HSTORE_FORMATTED =
      "\"consent_id\"=>\"3\", \"person_full_name\"=>\"name\"";

  private Map<String, String> sysValues;
  private Map<String, Object> businessValues;

  @Mock
  private DataSource dataSource;
  @Mock
  private Connection connection;
  @Mock
  private CallableStatement callableStatement;
  @Mock
  private Array rolesDbArray;
  @Mock
  private ResultSet resultSet;

  private DmlOperationHandler dmlOperationHandler;

  @BeforeEach
  void beforeEach() throws SQLException {
    dmlOperationHandler = new DmlOperationHandler(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareCall(any())).thenReturn(callableStatement);

    businessValues = new LinkedHashMap<>();
    businessValues.put("consent_id", 3);
    businessValues.put("person_full_name", "name");

    sysValues = new LinkedHashMap<>();
    sysValues.put("curr_user", "2");
    sysValues.put("source_system", "system");
  }

  @Test
  void expectSaveReturnInsertedId() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);
    when(callableStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(any())).thenReturn(ENTITY_ID);

    String actual = dmlOperationHandler.save(
        DmlOperationArgs.builder(TABLE_NAME, getMockedClaims(), sysValues)
            .saveOperationArgs(businessValues)
            .build());

    assertThat(actual).isEqualTo(ENTITY_ID);

    verify(callableStatement).executeQuery();
    verify(callableStatement).setString(1, TABLE_NAME);
    verify(callableStatement).setString(2, SYS_VALUES_HSTORE_FORMATTED);
    verify(callableStatement).setString(3, BUSINESS_VALUES_HSTORE_FORMATTED);
    verify(callableStatement).setArray(4, rolesDbArray);
  }

  @Test
  void expectExceptionWhenSaveNotReturnInsertedId() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);
    when(callableStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    JwtClaimsDto userClaims = getMockedClaims();

    ProcedureErrorException e = assertThrows(ProcedureErrorException.class,
        () -> dmlOperationHandler.save(
            DmlOperationArgs.builder(TABLE_NAME, userClaims, sysValues)
                .saveOperationArgs(businessValues)
                .build()));

    assertThat(e.getKafkaResponseStatus()).isEqualTo(Status.PROCEDURE_ERROR);
    assertThat(e.getDetails()).isNull();
  }

  @Test
  void expectUpdateExecutedSuccessfully() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);

    dmlOperationHandler.update(
        DmlOperationArgs.builder(TABLE_NAME, getMockedClaims(), sysValues)
            .updateOperationArgs(ENTITY_ID, businessValues)
            .build());

    verify(callableStatement).execute();
    verify(callableStatement).setString(1, TABLE_NAME);
    verify(callableStatement).setString(2, ENTITY_ID);
    verify(callableStatement).setString(3, SYS_VALUES_HSTORE_FORMATTED);
    verify(callableStatement).setString(4, BUSINESS_VALUES_HSTORE_FORMATTED);
    verify(callableStatement).setArray(5, rolesDbArray);
  }

  @Test
  void expectExceptionWhenUpdateSqlExceptionConstraint() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);
    when(callableStatement.execute()).thenThrow(new SQLException("", "23503"));
    JwtClaimsDto userClaims = getMockedClaims();

    ConstraintViolationException e = assertThrows(ConstraintViolationException.class,
        () -> dmlOperationHandler.update(
            DmlOperationArgs.builder(TABLE_NAME, userClaims, sysValues)
                .updateOperationArgs(ENTITY_ID, businessValues)
                .build()));

    assertThat(e.getKafkaResponseStatus()).isEqualTo(Status.CONSTRAINT_VIOLATION);
    assertThat(e.getDetails()).isEqualTo("foreign key");
  }

  @Test
  void expectDeleteExecutedSuccessfully() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);

    dmlOperationHandler.delete(
        DmlOperationArgs.builder(TABLE_NAME, getMockedClaims(), sysValues)
            .deleteOperationArgs(ENTITY_ID)
            .build());

    verify(callableStatement).execute();
    verify(callableStatement).setString(1, TABLE_NAME);
    verify(callableStatement).setString(2, ENTITY_ID);
    verify(callableStatement).setString(3, SYS_VALUES_HSTORE_FORMATTED);
    verify(callableStatement).setArray(4, rolesDbArray);
  }

  @Test
  void expectExceptionWhenDeleteSqlExceptionWithNoState() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);
    when(callableStatement.execute()).thenThrow(new SQLException(""));
    JwtClaimsDto userClaims = getMockedClaims();

    ProcedureErrorException e = assertThrows(ProcedureErrorException.class,
        () -> dmlOperationHandler.delete(
            DmlOperationArgs.builder(TABLE_NAME, userClaims, sysValues)
                .deleteOperationArgs(ENTITY_ID)
                .build()));

    assertThat(e.getKafkaResponseStatus()).isEqualTo(Status.PROCEDURE_ERROR);
    assertThat(e.getDetails()).isNull();
  }

  @Test
  void expectExceptionWhenDeleteSqlExceptionWithPermissionDenied() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray())).thenReturn(rolesDbArray);
    when(callableStatement.execute()).thenThrow(new SQLException("", "20003"));
    JwtClaimsDto userClaims = getMockedClaims();

    ForbiddenOperationException e = assertThrows(ForbiddenOperationException.class,
        () -> dmlOperationHandler.delete(
            DmlOperationArgs.builder(TABLE_NAME, userClaims, sysValues)
                .deleteOperationArgs(ENTITY_ID)
                .build()));

    assertThat(e.getKafkaResponseStatus()).isEqualTo(Status.FORBIDDEN_OPERATION);
    assertThat(e.getDetails()).isNull();
  }

  private JwtClaimsDto getMockedClaims() {
    JwtClaimsDto userClaims = new JwtClaimsDto();
    RolesDto rolesDto = new RolesDto();
    rolesDto.setRoles(ROLES);
    userClaims.setRealmAccess(rolesDto);
    return userClaims;
  }
}
