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

package com.epam.digital.data.platform.kafkaapi.core.commandhandler.impl;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.AbstractUpsertCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.tabledata.MockEntityTableDataProviderImpl;
import com.epam.digital.data.platform.kafkaapi.core.util.MockEntity;
import org.springframework.boot.test.context.TestComponent;

@TestComponent
public class UpsertCommandHandlerTestImpl extends AbstractUpsertCommandHandler<MockEntity> {

  public UpsertCommandHandlerTestImpl(
      EntityConverter<MockEntity> entityConverter,
      MockEntityTableDataProviderImpl tableDataProvider,
      CreateCommandHandlerTestImpl createCommandHandler,
      UpdateCommandHandlerTestImpl updateCommandHandler) {
    super(entityConverter, tableDataProvider, createCommandHandler, updateCommandHandler);
  }
}
