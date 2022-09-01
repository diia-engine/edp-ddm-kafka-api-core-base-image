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

package com.epam.digital.data.platform.kafkaapi.core.commandhandler.model;

import com.epam.digital.data.platform.starter.security.dto.JwtClaimsDto;
import java.util.Map;
import java.util.Objects;

public class DmlOperationArgs {

  private final String tableName;
  private final JwtClaimsDto userClaims;
  private final String entityId;
  private final Map<String, String> sysValues;
  private final Map<String, Object> businessValues;

  private DmlOperationArgs(Builder builder) {
    tableName = builder.tableName;
    userClaims = builder.userClaims;
    entityId = builder.entityId;
    sysValues = builder.sysValues;
    businessValues = builder.businessValues;
  }

  public String getTableName() {
    return tableName;
  }

  public JwtClaimsDto getUserClaims() {
    return userClaims;
  }

  public String getEntityId() {
    return entityId;
  }

  public Map<String, String> getSysValues() {
    return sysValues;
  }

  public Map<String, Object> getBusinessValues() {
    return businessValues;
  }

  public static InitialBuilder builder(String tableName, JwtClaimsDto userClaims,
      Map<String, String> sysValues) {
    return new InitialBuilder(tableName, userClaims, sysValues);
  }

  public static final class InitialBuilder {

    private final Builder builder;

    private InitialBuilder(String tableName, JwtClaimsDto userClaims,
        Map<String, String> sysValues) {
      this.builder = new Builder(tableName, userClaims, sysValues);
    }

    public Builder deleteOperationArgs(String entityId) {
      builder.setEntityId(entityId);
      return builder;
    }

    public Builder saveOperationArgs(Map<String, Object> businessValues) {
      builder.setBusinessValues(businessValues);
      return builder;
    }

    public Builder updateOperationArgs(String entityId, Map<String, Object> businessValues) {
      builder.setEntityId(entityId);
      builder.setBusinessValues(businessValues);
      return builder;
    }
  }

  public static final class Builder {

    private final String tableName;
    private final JwtClaimsDto userClaims;
    private String entityId;
    private final Map<String, String> sysValues;
    private Map<String, Object> businessValues;

    private Builder(String tableName, JwtClaimsDto userClaims,
        Map<String, String> sysValues) {
      this.tableName = tableName;
      this.userClaims = userClaims;
      this.sysValues = sysValues;
    }

    private void setEntityId(String entityId) {
      this.entityId = entityId;
    }

    private void setBusinessValues(Map<String, Object> businessValues) {
      this.businessValues = businessValues;
    }

    public DmlOperationArgs build() {
      return new DmlOperationArgs(this);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DmlOperationArgs that = (DmlOperationArgs) o;
    return Objects.equals(tableName, that.tableName) && Objects
        .equals(userClaims, that.userClaims) && Objects.equals(entityId, that.entityId)
        && Objects.equals(sysValues, that.sysValues) && Objects
        .equals(businessValues, that.businessValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, userClaims, entityId, sysValues, businessValues);
  }
}
