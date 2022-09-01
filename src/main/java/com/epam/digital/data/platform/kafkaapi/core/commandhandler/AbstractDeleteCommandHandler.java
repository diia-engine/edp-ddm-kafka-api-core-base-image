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

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.model.DmlOperationArgs;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.DmlOperationHandler;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.service.JwtInfoProvider;
import com.epam.digital.data.platform.kafkaapi.core.tabledata.TableDataProvider;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public abstract class AbstractDeleteCommandHandler<T> implements DeleteCommandHandler<T> {

  @Autowired
  private JwtInfoProvider jwtInfoProvider;
  @Autowired
  private DmlOperationHandler dmlOperationHandler;

  private final EntityConverter<T> entityConverter;
  private final TableDataProvider tableDataProvider;

  protected AbstractDeleteCommandHandler(
      EntityConverter<T> entityConverter, TableDataProvider tableDataProvider) {
    this.entityConverter = entityConverter;
    this.tableDataProvider = tableDataProvider;
  }

  @Override
  public void delete(Request<T> input) {
    JwtClaimsDto userClaims = jwtInfoProvider.getUserClaims(input);
    String entityId =
        entityConverter.getUuidOfEntity(input.getPayload(), tableDataProvider.pkColumnName());
    Map<String, String> sysValues = entityConverter.buildSysValues(userClaims.getDrfo(), input);

    dmlOperationHandler.delete(
        DmlOperationArgs.builder(tableDataProvider.tableName(), userClaims, sysValues)
            .deleteOperationArgs(entityId)
            .build());
  }
}
