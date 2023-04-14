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

import com.epam.digital.data.platform.kafkaapi.core.audit.AuditableDatabaseOperation;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.model.DmlOperationArgs;
import com.epam.digital.data.platform.kafkaapi.core.exception.ProcedureErrorException;
import com.epam.digital.data.platform.kafkaapi.core.util.JwtClaimsUtils;
import com.epam.digital.data.platform.kafkaapi.core.util.Operation;
import com.epam.digital.data.platform.kafkaapi.core.util.SQLExceptionResolverUtil;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.postgresql.util.HStoreConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DmlOperationHandler {

  private static final String INSERT_ID_COLUMN = "f_row_insert";

  private final Logger log = LoggerFactory.getLogger(DmlOperationHandler.class);

  private final DataSource dataSource;

  public DmlOperationHandler(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @AuditableDatabaseOperation(Operation.CREATE)
  @Transactional
  public String save(DmlOperationArgs args) {
    log.info("Inserting into table {}", args.getTableName());

    var connection = DataSourceUtils.getConnection(dataSource);
    try (CallableStatement statement = connection.prepareCall(DmlOperation.I.getSqlString())) {
      Array rolesDbArray = connection
          .createArrayOf("text", JwtClaimsUtils.getRoles(args.getUserClaims()).toArray());
      statement.setString(1, args.getTableName()); //NOSONAR
      statement.setString(2, HStoreConverter.toString(args.getSysValues()));
      statement.setString(3, HStoreConverter.toString(args.getBusinessValues()));
      statement.setArray(4, rolesDbArray);

      ResultSet resultSet = statement.executeQuery();
      if (resultSet.next()) {
        return resultSet.getString(INSERT_ID_COLUMN);
      } else {
        throw new ProcedureErrorException(
            "Inserted ID is not returned from procedure in column " + INSERT_ID_COLUMN);
      }
    } catch (SQLException e) {
      throw SQLExceptionResolverUtil.getDetailedExceptionFromSql(e);
    }
  }

  @AuditableDatabaseOperation(Operation.UPDATE)
  @Transactional
  public void update(DmlOperationArgs args) {
    log.info("Updating table {}", args.getTableName());

    var connection = DataSourceUtils.getConnection(dataSource);
    try (CallableStatement statement = connection.prepareCall(DmlOperation.U.getSqlString())) {
      Array rolesDbArray = connection
          .createArrayOf("text", JwtClaimsUtils.getRoles(args.getUserClaims()).toArray());
      statement.setString(1, args.getTableName()); //NOSONAR
      statement.setString(2, args.getEntityId());
      statement.setString(3, HStoreConverter.toString(args.getSysValues()));
      statement.setString(4, HStoreConverter.toString(args.getBusinessValues()));
      statement.setArray(5, rolesDbArray);

      statement.execute();
    } catch (SQLException e) {
      throw SQLExceptionResolverUtil.getDetailedExceptionFromSql(e);
    }
  }

  @AuditableDatabaseOperation(Operation.DELETE)
  @Transactional
  public void delete(DmlOperationArgs args) {
    log.info("Deleting from  table {}", args.getTableName());

    var connection = DataSourceUtils.getConnection(dataSource);
    try (CallableStatement statement = connection.prepareCall(DmlOperation.D.getSqlString())) {
      Array rolesDbArray = connection
          .createArrayOf("text", JwtClaimsUtils.getRoles(args.getUserClaims()).toArray());
      statement.setString(1, args.getTableName()); //NOSONAR
      statement.setString(2, args.getEntityId());
      statement.setString(3, HStoreConverter.toString(args.getSysValues()));
      statement.setArray(4, rolesDbArray);

      statement.execute();
    } catch (SQLException e) {
      throw SQLExceptionResolverUtil.getDetailedExceptionFromSql(e);
    }
  }
}
