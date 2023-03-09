/*
 * Copyright 2022 EPAM Systems.
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

import com.epam.digital.data.platform.kafkaapi.core.exception.PatternException;
import com.epam.digital.data.platform.kafkaapi.core.model.Sequence;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public abstract class AbstractSequenceProvider {

  private static final String SEQ = "{SEQ}";
  private static final String DATE_TIME = "\\{.+}";
  private static final String MESSAGE = "Value '%s' does not match pattern '%s'";

  @Autowired
  private SequenceGenerator sequenceGenerator;

  public String generate(String columnName) {
    Sequence sequence = columnSequence().get(columnName);
    String pattern = sequence.getPattern();
    if (pattern.contains("{SEQ}")) {
      Long nextValue = sequenceGenerator.nextValue(sequence.getName());
      pattern = pattern.replace("{SEQ}", nextValue.toString());
    }
    while (pattern.contains("{") && pattern.contains("}")
        && pattern.indexOf("{") < pattern.indexOf("}")) {

      int beginIndex = pattern.indexOf("{");
      int endIndex = pattern.indexOf("}");
      String dateTimePattern = pattern.substring(beginIndex + 1, endIndex);
      try {
        var dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
        pattern = pattern.replace("{" + dateTimePattern + "}",
            dateTimeFormatter.format(LocalDateTime.now()));
      } catch (IllegalArgumentException e) {
        var message = String.format("Cannot render date-time with pattern '%s': %s",
            dateTimePattern, e.getMessage());
        throw new PatternException(message, e);
      }
    }
    return pattern;
  }

  public void validate(String columnName, String value) {
    String pattern = columnSequence().get(columnName).getPattern();
    boolean isValid;
    try {
      var regexp = pattern.replace(SEQ, "\\d+");
      regexp = regexp.replaceAll(DATE_TIME, ".+");
      isValid = value.matches(regexp);
    } catch (Exception e) {
      throw new PatternException(String.format(MESSAGE, value, pattern), e);
    }
    if (!isValid) {
      throw new PatternException(String.format(MESSAGE, value, pattern));
    }
  }

  protected abstract Map<String, Sequence> columnSequence();
}
