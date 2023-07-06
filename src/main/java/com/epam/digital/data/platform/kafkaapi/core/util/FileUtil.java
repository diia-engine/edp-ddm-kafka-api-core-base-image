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

package com.epam.digital.data.platform.kafkaapi.core.util;

import com.epam.digital.data.platform.kafkaapi.core.exception.ChecksumInconsistencyException;
import com.epam.digital.data.platform.model.core.kafka.File;
import java.io.ByteArrayInputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class FileUtil {

  private static final Logger log = LoggerFactory.getLogger(FileUtil.class);

  public static void validateChecksum(File file, byte[] content) {
    var calculatedChecksum = DigestUtils.sha256Hex(content);

    if (!StringUtils.equals(calculatedChecksum, file.getChecksum())) {
      throw new ChecksumInconsistencyException(
          String.format(
              "Checksum from ceph object (%s) and from request (%s) do not match. File id: '%s'",
              calculatedChecksum, file.getChecksum(), file.getId()));
    }
  }

  public static byte[] getContent(InputStream inputStream) {
    log.info("Getting file content");
    try {
      return IOUtils.toByteArray(inputStream);
    } catch (IOException e) {
      throw new IllegalArgumentException("Couldn't read returned ceph content from stream", e);
    }
  }

  public static void resetContent(InputStream inputStream) {
    try {
      inputStream.reset();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Couldn't reset input stream returned ceph content from stream", e);
    }
  }

}
