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

package com.epam.digital.data.platform.kafkaapi.core.service;

import com.epam.digital.data.platform.kafkaapi.core.config.JooqTestConfig;
import com.epam.digital.data.platform.kafkaapi.core.exception.ForbiddenOperationException;
import com.epam.digital.data.platform.kafkaapi.core.model.FieldsAccessCheckDto;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import com.epam.digital.data.platform.model.core.kafka.Status;
import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;
import com.epam.digital.data.platform.starter.security.dto.RolesDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = {AccessPermissionService.class, JooqTestConfig.class})
class AccessPermissionServiceTest {

  private static final String TABLE_NAME = "table";
  private static final List<String> ROLES = singletonList("role");

  @Autowired
  private AccessPermissionService accessPermissionService;

  @MockBean
  private DataSource dataSource;

  @Mock
  private Connection connection;
  @Mock
  private CallableStatement callableStatement;
  @Mock
  private Array rolesDbArray;
  @Mock
  private Array requestedFieldsDbArray;
  @Mock
  private ResultSet resultSet;

  @BeforeEach
  void beforeEach() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareCall(any())).thenReturn(callableStatement);
  }

  @Test
  void expectHasAccessIfReturnedFromDb() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray()))
            .thenReturn(rolesDbArray);
    when(connection.createArrayOf(
            "text",
            new String[] {
              "consent_id",
              "consent_date",
              "person_full_name",
              "person_pass_number",
              "passport_scan_copy"
            }))
        .thenReturn(requestedFieldsDbArray);
    when(callableStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getBoolean(1)).thenReturn(true);
    JwtClaimsDto userClaims = getMockedClaims();

    boolean actual =
        accessPermissionService.hasReadAccess(
            List.of(
                new FieldsAccessCheckDto(
                    TABLE_NAME,
                    List.of(
                        "consent_id",
                        "consent_date",
                        "person_full_name",
                        "person_pass_number",
                        "passport_scan_copy"))),
            userClaims);

    assertThat(actual).isTrue();

    verify(callableStatement).setString(1, TABLE_NAME);
    verify(callableStatement).setArray(2, rolesDbArray);
    verify(callableStatement).setString(3, "S");
    verify(callableStatement).setArray(4, requestedFieldsDbArray);
  }

  @Test
  void expectNoAccessIfNoResultSetInResponse() throws SQLException {
    when(connection.createArrayOf("text", ROLES.toArray()))
            .thenReturn(rolesDbArray);
    when(connection.createArrayOf(
            "text",
            new String[] {
              "consent_id",
              "consent_date",
              "person_full_name",
              "person_pass_number",
              "passport_scan_copy"
            }))
        .thenReturn(requestedFieldsDbArray);
    when(callableStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);
    JwtClaimsDto userClaims = getMockedClaims();

    boolean actual =
        accessPermissionService.hasReadAccess(
            List.of(
                new FieldsAccessCheckDto(
                    TABLE_NAME,
                    List.of(
                        "consent_id",
                        "consent_date",
                        "person_full_name",
                        "person_pass_number",
                        "passport_scan_copy"))),
            userClaims);

    assertThat(actual).isFalse();
  }

  @Test
  void expectCustomExceptionWhenSqlExceptionResolved() throws SQLException {
    when(callableStatement.executeQuery()).thenThrow(new SQLException("", "20003"));
    JwtClaimsDto userClaims = getMockedClaims();

    ForbiddenOperationException e =
        assertThrows(
            ForbiddenOperationException.class,
            () ->
                accessPermissionService.hasReadAccess(
                    List.of(
                        new FieldsAccessCheckDto(
                            TABLE_NAME,
                            List.of(
                                "consent_id",
                                "consent_date",
                                "person_full_name",
                                "person_pass_number",
                                "passport_scan_copy"))),
                    userClaims));

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
