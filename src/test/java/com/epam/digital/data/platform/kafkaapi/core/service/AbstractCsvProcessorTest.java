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

import com.epam.digital.data.platform.kafkaapi.core.config.CsvConfig;
import com.epam.digital.data.platform.kafkaapi.core.exception.CsvDtoValidationException;
import com.epam.digital.data.platform.kafkaapi.core.exception.CsvFileEncodingException;
import com.epam.digital.data.platform.kafkaapi.core.exception.CsvFileParsingException;
import com.epam.digital.data.platform.kafkaapi.core.service.impl.CsvProcessorTestImpl;
import com.epam.digital.data.platform.storage.file.dto.FileDataDto;
import com.fasterxml.jackson.dataformat.csv.CsvReadException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(
    classes = {
            CsvConfig.class,
            LocalValidatorFactoryBean.class,
            CsvProcessorTestImpl.class
    })
class AbstractCsvProcessorTest {

  @Autowired
  private CsvProcessorTestImpl instance;

  @Test
  void expectValidFileProcessedToPayload() {
    FileDataDto fileDataDto = FileDataDto.builder()
            .content(AbstractCsvProcessorTest.class.getResourceAsStream("/csv/mockEntity.csv"))
            .build();

    var actualPayload = instance.transformFileToEntities(fileDataDto);

    assertThat(actualPayload.size()).isEqualTo(2);

    assertThat(actualPayload.get(0).getConsentDate())
        .isEqualTo(LocalDateTime.of(2021, 1, 29, 11, 8, 16, 631000000));
    assertThat(actualPayload.get(0).getPersonFullName()).isEqualTo("Name");
    assertThat(actualPayload.get(0).getPersonPassNumber()).isEqualTo("АА111132");
    assertThat(actualPayload.get(0).getPassportScanCopy()).isNull();

    assertThat(actualPayload.get(1).getConsentDate())
        .isEqualTo(LocalDateTime.of(2021, 1, 30, 11, 8, 16, 631000000));
    assertThat(actualPayload.get(1).getPersonFullName()).isEqualTo("Name2");
    assertThat(actualPayload.get(1).getPersonPassNumber()).isEqualTo("АА111133");
    assertThat(actualPayload.get(1).getPassportScanCopy()).isNull();
  }

  @Test
  void expectExceptionOnInvalidEncoding() {
    FileDataDto fileDataDto = FileDataDto.builder()
            .content(
                    AbstractCsvProcessorTest.class.getResourceAsStream(
                            "/csv/mockEntityInvalidEncoding.csv"))
            .build();

    assertThrows(
        CsvFileEncodingException.class, () -> instance.transformFileToEntities(fileDataDto));
  }

  @Test
  void expectExceptionOnInvalidCsvContent() {
    FileDataDto fileDataDto = FileDataDto.builder()
            .content(
                    AbstractCsvProcessorTest.class.getResourceAsStream(
                            "/csv/mockEntityInvalidCsvFormat.csv"))
            .build();

    var actualException =
        assertThrows(CsvFileParsingException.class,
            () -> instance.transformFileToEntities(fileDataDto));
    assertThat(actualException.getCause()).isExactlyInstanceOf(CsvReadException.class);
  }

  @Test
  void expectValidationExceptionOnCsvContentDto() {
    FileDataDto fileDataDto = FileDataDto.builder()
            .content(
                    AbstractCsvProcessorTest.class.getResourceAsStream(
                            "/csv/mockEntityInvalidPassFormat.csv"))
            .build();

    var actualException =
        assertThrows(CsvDtoValidationException.class,
            () -> instance.transformFileToEntities(fileDataDto));
    assertThat(actualException.getBindingResult().getErrorCount()).isEqualTo(1);
    assertThat(actualException.getBindingResult().getFieldErrors().get(0).getField())
        .isEqualTo("entities[0].personPassNumber");
  }
}
