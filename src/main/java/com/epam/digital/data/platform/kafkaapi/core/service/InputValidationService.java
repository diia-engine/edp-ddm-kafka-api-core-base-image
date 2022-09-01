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

package com.epam.digital.data.platform.kafkaapi.core.service;

import com.epam.digital.data.platform.kafkaapi.core.model.ValidationResult;
import com.epam.digital.data.platform.model.core.kafka.Request;
import com.epam.digital.data.platform.model.core.kafka.Status;
import org.springframework.stereotype.Component;

@Component
public class InputValidationService {

  private final DigitalSignatureService signatureService;
  private final JwtValidationService jwtValidationService;

  public InputValidationService(
      DigitalSignatureService signatureService, JwtValidationService jwtValidationService) {
    this.signatureService = signatureService;
    this.jwtValidationService = jwtValidationService;
  }

  public <T> ValidationResult validate(String key, Request<T> input) {
    if (!jwtValidationService.isValid(input)) {
      return new ValidationResult(false, Status.JWT_INVALID);
    }

    if (!signatureService.isSealValid(key, input)) {
      return new ValidationResult(false, Status.INVALID_SIGNATURE);
    }

    return new ValidationResult(true);
  }
}
