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

import com.epam.digital.data.platform.kafkaapi.core.exception.CsvDtoValidationException;
import com.epam.digital.data.platform.kafkaapi.core.exception.CsvFileEncodingException;
import com.epam.digital.data.platform.kafkaapi.core.exception.CsvFileParsingException;
import com.epam.digital.data.platform.kafkaapi.core.util.FileUtil;
import com.epam.digital.data.platform.storage.file.dto.FileDataDto;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.parser.txt.CharsetDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Validator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractCsvProcessor<V, U> implements CsvProcessor<V> {
    private final Logger log = LoggerFactory.getLogger(AbstractCsvProcessor.class);
    @Autowired
    private Function<Class<?>, ObjectReader> csvReaderFactory;
    @Autowired
    private Validator validator;

    public List<V> transformFileToEntities(FileDataDto fileDataDto) {
        var content = FileUtil.getContent(fileDataDto.getContent());
        validateContent(content);

        List<V> objectsFromContent = getObjectsFromContent(content);
        validateObjects(objectsFromContent);
        return objectsFromContent;
    }

    private void validateContent(byte[] content) {
        log.info("Validating csv file content");
        CharsetDetector charsetDetector = new CharsetDetector(content.length);
        charsetDetector.setText(content);
        var encoding = charsetDetector.detectAll()[0].getName();
        if (!StringUtils.equals(encoding, StandardCharsets.UTF_8.name())) {
            throw new CsvFileEncodingException(
                    "Wrong csv file encoding found instead of UTF-8: " + encoding);
        }
    }

    public List<V> getObjectsFromContent(byte[] content) {
        log.info("Processing csv file content");
        var reader = csvReaderFactory.apply(getCsvRowElementType());
        try (MappingIterator<V> csvRowsContent = reader.readValues(content)) {
            return csvRowsContent.readAll();
        } catch (IOException exception) {
            throw new CsvFileParsingException("Exception on parsing csv file content", exception);
        }
    }

    private void validateObjects(List<V> payloadObject) {
        log.info("Validating dto retrieved from csv file content");
        U target = getPayloadObjectFromCsvRows(payloadObject);
        var errors = new BeanPropertyBindingResult(target, target.getClass().getName());
        validator.validate(target, errors);
        if (errors.hasErrors()) {
            throw new CsvDtoValidationException("Failed validation of csv file content", errors);
        }
    }

    protected abstract Class<V> getCsvRowElementType();

    protected abstract U getPayloadObjectFromCsvRows(List<V> rows);
}