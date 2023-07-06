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

package com.epam.digital.data.platform.kafkaapi.core.service;

import com.epam.digital.data.platform.kafkaapi.core.util.FileUtil;
import com.epam.digital.data.platform.model.core.kafka.File;
import com.epam.digital.data.platform.storage.file.dto.FileDataDto;
import com.epam.digital.data.platform.storage.file.exception.FileNotFoundException;
import com.epam.digital.data.platform.storage.file.service.FormDataFileKeyProvider;
import com.epam.digital.data.platform.storage.file.service.FormDataFileStorageService;
import java.io.IOException;
import java.util.Objects;
import org.eclipse.rdf4j.common.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.util.Optional;

@Component
public class FileService {

  private final Logger log = LoggerFactory.getLogger(FileService.class);

  private final FormDataFileStorageService lowcodeFileDataStorageService;
  private final FormDataFileStorageService datafactoryFileDataStorageService;
  private final FormDataFileKeyProvider fileKeyProvider;

  public FileService(
      @Qualifier("lowcodeFileDataStorageService") FormDataFileStorageService lowcodeFileDataStorageService,
      @Qualifier("datafactoryFileDataStorageService") FormDataFileStorageService datafactoryFileDataStorageService,
      FormDataFileKeyProvider fileKeyProvider) {
    this.lowcodeFileDataStorageService = lowcodeFileDataStorageService;
    this.datafactoryFileDataStorageService = datafactoryFileDataStorageService;
    this.fileKeyProvider = fileKeyProvider;
  }

  public Optional<FileDataDto> retrieve(String instanceId, File file) {
    if (Objects.isNull(file)) {
      return Optional.empty();
    }
    log.info("Storing file '{}' from lowcode to data ceph bucket", file.getId());

    var lowcodeId = fileKeyProvider.generateKey(instanceId, file.getId());

    FileDataDto fileDataDto;
    try {
      fileDataDto = lowcodeFileDataStorageService.loadByKey(lowcodeId);
    } catch (FileNotFoundException ex) {
      log.warn("File not found ", ex);
      return Optional.empty();
    }

    var content = FileUtil.getContent(fileDataDto.getContent());
    FileUtil.validateChecksum(file, content);

    fileDataDto.setContent(new ByteArrayInputStream(content));
    datafactoryFileDataStorageService.save(file.getId(), fileDataDto);
    FileUtil.resetContent(fileDataDto.getContent());
    return Optional.of(fileDataDto);
  }
}
