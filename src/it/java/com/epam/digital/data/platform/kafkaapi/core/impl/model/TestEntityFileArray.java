/*
 * Copyright 2023 EPAM Systems.
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

package com.epam.digital.data.platform.kafkaapi.core.impl.model;

import com.epam.digital.data.platform.model.core.kafka.File;
import java.util.List;
import java.util.UUID;

public class TestEntityFileArray {

  private UUID id;
  private String legalEntityName;
  private List<File> scanCopies;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getLegalEntityName() {
    return legalEntityName;
  }

  public void setLegalEntityName(String legalEntityName) {
    this.legalEntityName = legalEntityName;
  }

  public List<File> getScanCopies() {
    return scanCopies;
  }

  public void setScanCopies(List<File> scanCopies) {
    this.scanCopies = scanCopies;
  }
}
