/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;

public class BadJsonQueryException extends BadQueryException
{
  public static final String ERROR_CLASS = JsonParseException.class.getName();

  public BadJsonQueryException(JsonParseException e)
  {
    this(e, JSON_PARSE_ERROR_CODE, e.getMessage(), ERROR_CLASS);
  }

  @JsonCreator
  private BadJsonQueryException(
      @JsonProperty("error") String errorCode,
      @JsonProperty("errorMessage") String errorMessage,
      @JsonProperty("errorClass") String errorClass
  )
  {
    this(null, errorCode, errorMessage, errorClass);
  }

  private BadJsonQueryException(
      Throwable cause,
      String errorCode,
      String errorMessage,
      String errorClass
  )
  {
    super(cause, errorCode, errorMessage, errorClass, null);
  }
}
