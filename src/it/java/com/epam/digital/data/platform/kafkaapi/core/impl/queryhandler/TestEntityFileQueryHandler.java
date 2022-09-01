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

package com.epam.digital.data.platform.kafkaapi.core.impl.queryhandler;

import com.epam.digital.data.platform.kafkaapi.core.impl.model.TestEntityFile;
import com.epam.digital.data.platform.kafkaapi.core.impl.tabledata.TestEntityFileTableDataProvider;
import com.epam.digital.data.platform.kafkaapi.core.model.FieldsAccessCheckDto;
import com.epam.digital.data.platform.kafkaapi.core.queryhandler.AbstractQueryHandler;
import com.epam.digital.data.platform.kafkaapi.core.util.JooqDataTypes;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.impl.DSL;

public class TestEntityFileQueryHandler extends AbstractQueryHandler<UUID, TestEntityFile> {

  public TestEntityFileQueryHandler(TestEntityFileTableDataProvider tableDataProvider) {
    super(tableDataProvider);
  }

  @Override
  public List<FieldsAccessCheckDto> getFieldsToCheckAccess() {
    return List.of(
        new FieldsAccessCheckDto(
            "test_entity_file", List.of("id", "legal_entity_name", "scan_copy")));
  }

  @Override
  public Class<TestEntityFile> entityType() {
    return TestEntityFile.class;
  }

  @Override
  public List<SelectFieldOrAsterisk> selectFields() {
    return Arrays.asList(
        DSL.field("id"),
        DSL.field("legal_entity_name"),
        DSL.field("scan_copy", JooqDataTypes.FILE_DATA_TYPE
        ));
  }
}
