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

package com.epam.digital.data.platform.kafkaapi.core.util;

import com.epam.digital.data.platform.model.core.geometry.Geometry;
import com.epam.digital.data.platform.model.core.kafka.File;
import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.LocalDateTime;
import java.util.UUID;
import javax.validation.constraints.Pattern;

public class MockEntity {

  private UUID consentId;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private LocalDateTime consentDate;

  private String personFullName;

  @Pattern(regexp = "^[АВЕІКМНОРСТХ]{2}[0-9]{6}$")
  private String personPassNumber;

  private File passportScanCopy;

  private Geometry location;

  public UUID getConsentId() {
    return this.consentId;
  }

  public void setConsentId(UUID consentId) {
    this.consentId = consentId;
  }

  public LocalDateTime getConsentDate() {
    return this.consentDate;
  }

  public void setConsentDate(LocalDateTime consentDate) {
    this.consentDate = consentDate;
  }

  public String getPersonFullName() {
    return this.personFullName;
  }

  public void setPersonFullName(String personFullName) {
    this.personFullName = personFullName;
  }

  public String getPersonPassNumber() {
    return this.personPassNumber;
  }

  public void setPersonPassNumber(String personPassNumber) {
    this.personPassNumber = personPassNumber;
  }

  public File getPassportScanCopy() {
    return passportScanCopy;
  }

  public void setPassportScanCopy(File passportScanCopy) {
    this.passportScanCopy = passportScanCopy;
  }

  public Geometry getLocation() {
    return location;
  }

  public void setLocation(Geometry location) {
    this.location = location;
  }
}
