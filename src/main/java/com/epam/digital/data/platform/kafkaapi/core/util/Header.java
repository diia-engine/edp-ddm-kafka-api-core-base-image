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

public class Header {

  private Header() {
  }

  public static final String X_SOURCE_SYSTEM = "x-source-system";
  public static final String X_SOURCE_APPLICATION = "x-source-application";
  public static final String X_SOURCE_BUSINESS_PROCESS = "x-source-business-process";
  public static final String X_SOURCE_BUSINESS_PROCESS_INSTANCE_ID =
      "x-source-business-process-instance-id";
  public static final String X_SOURCE_BUSINESS_PROCESS_DEFINITION_ID =
      "x-source-business-process-definition-id";
  public static final String X_SOURCE_BUSINESS_ACTIVITY = "x-source-business-activity";
  public static final String X_SOURCE_BUSINESS_ACTIVITY_INSTANCE_ID =
      "x-source-business-activity-instance-id";

  public static final String TRACE_ID = "X-B3-TraceId";

  public static final String DIGITAL_SEAL = "digital-seal";
}
