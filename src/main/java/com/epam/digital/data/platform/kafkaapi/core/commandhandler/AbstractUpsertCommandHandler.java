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

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.tabledata.TableDataProvider;
import com.epam.digital.data.platform.model.core.kafka.EntityId;
import com.epam.digital.data.platform.model.core.kafka.Request;
import java.util.Map;
import java.util.UUID;

public abstract class AbstractUpsertCommandHandler<T> implements UpsertCommandHandler<T> {

  private final CreateCommandHandler<T> createCommandHandler;
  private final UpdateCommandHandler<T> updateCommandHandler;
  private final EntityConverter<T> entityConverter;
  private final TableDataProvider tableDataProvider;

  protected AbstractUpsertCommandHandler(
      EntityConverter<T> entityConverter,
      TableDataProvider tableDataProvider,
      CreateCommandHandler<T> createCommandHandler,
      UpdateCommandHandler<T> updateCommandHandler) {
    this.entityConverter = entityConverter;
    this.tableDataProvider = tableDataProvider;
    this.createCommandHandler = createCommandHandler;
    this.updateCommandHandler = updateCommandHandler;
  }
  
  @Override
  public EntityId upsert(Request<T> input) {
    Map<String, Object> entityMap = entityConverter.entityToMap(input.getPayload());
    Object entityId = entityMap.remove(tableDataProvider.pkColumnName());
    if (entityId == null) {
      return createCommandHandler.save(input);
    }
    updateCommandHandler.update(input);
    return new EntityId(UUID.fromString(entityId.toString()));
  }
}
