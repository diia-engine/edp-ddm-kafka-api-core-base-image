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

package com.epam.digital.data.platform.kafkaapi.core.impl.commandhandler;

import com.epam.digital.data.platform.kafkaapi.core.commandhandler.AbstractCreateCommandHandler;
import com.epam.digital.data.platform.kafkaapi.core.commandhandler.util.EntityConverter;
import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntityM2M;
import com.epam.digital.data.platform.kafkaapi.core.impl.tabledata.TestEntityM2MTableDataProvider;
import org.springframework.stereotype.Service;

@Service
public class TestEntityM2MCreateCommandHandler extends AbstractCreateCommandHandler<TestEntityM2M> {

  public TestEntityM2MCreateCommandHandler(
      EntityConverter<TestEntityM2M> entityConverter,
      TestEntityM2MTableDataProvider tableDataProvider) {
    super(entityConverter, tableDataProvider);
  }
}
