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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.table.BaseTableFunction;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.RowSignature;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Registers the "EXTERN" operator, which is used in queries like
 * <pre>{@code
 * INSERT INTO dst SELECT * FROM TABLE(EXTERN(
 *   "<input source>",
 *   "<input format>",
 *   "<signature>"))
 *
 * INSERT INTO dst SELECT * FROM TABLE(EXTERN(
 *   inputSource => "<input source>",
 *   inputFormat => "<input format>"))
 *   EXTEND (<columns>)
 * }</pre>
 * Where either the by-position or by-name forms are usable with either
 * a Druid JSON signature, or an SQL {@code EXTEND} list of columns.
 * As with all table functions, the {@code EXTEND} is optional.
 */
public class ExternalOperatorConversion extends DruidExternTableMacroConversion
{
  public static final String FUNCTION_NAME = "EXTERN";

  public static final String INPUT_SOURCE_PARAM = "inputSource";
  public static final String INPUT_FORMAT_PARAM = "inputFormat";
  public static final String SIGNATURE_PARAM = "signature";

  /**
   * The use of a table function allows the use of optional arguments,
   * so that the signature can be given either as the original-style
   * serialized JSON signature, or the updated SQL-style EXTEND clause.
   */
  private static class ExternFunction extends BaseTableFunction
  {
    public ExternFunction()
    {
      super(Arrays.asList(
          new Parameter(INPUT_SOURCE_PARAM, ParameterType.VARCHAR, false),
          new Parameter(INPUT_FORMAT_PARAM, ParameterType.VARCHAR, false),

          // Optional: the user can either provide the signature OR
          // an EXTEND clause. Checked in the implementation.
          new Parameter(SIGNATURE_PARAM, ParameterType.VARCHAR, true)
      ));
    }

    @Override
    public ExternalTableSpec apply(
        final String fnName,
        final Map<String, Object> args,
        final List<ColumnSpec> columns,
        final ObjectMapper jsonMapper
    )
    {
      try {
        final String sigValue = CatalogUtils.getString(args, SIGNATURE_PARAM);
        if (sigValue == null && columns == null) {
          throw new IAE(
              "EXTERN requires either a %s value or an EXTEND clause",
              SIGNATURE_PARAM
          );
        }
        if (sigValue != null && columns != null) {
          throw new IAE(
              "EXTERN requires either a %s value or an EXTEND clause, but not both",
              SIGNATURE_PARAM
          );
        }
        final RowSignature rowSignature;
        if (columns != null) {
          try {
            rowSignature = Columns.convertSignature(columns);
          }
          catch (IAE e) {
            throw new ArgumentAndException("columns", e);
          }
        } else {
          try {
            rowSignature = jsonMapper.readValue(sigValue, RowSignature.class);
          }
          catch (JsonProcessingException e) {
            throw new ArgumentAndException("rowSignature", e);
          }
        }

        String inputSrcStr = CatalogUtils.getString(args, INPUT_SOURCE_PARAM);
        InputSource inputSource;
        try {
          inputSource = jsonMapper.readValue(inputSrcStr, InputSource.class);
        }
        catch (JsonProcessingException e) {
          throw new ArgumentAndException("rowSignature", e);
        }
        InputFormat inputFormat;
        try {
          inputFormat = jsonMapper.readValue(CatalogUtils.getString(args, INPUT_FORMAT_PARAM), InputFormat.class);
        }
        catch (JsonProcessingException e) {
          throw new ArgumentAndException("inputFormat", e);
        }
        return new ExternalTableSpec(
            inputSource,
            inputFormat,
            rowSignature,
            inputSource::getTypes
        );
      }
      catch (ArgumentAndException e) { // We can triage out the error to one of the argument passed to the EXTERN function
        throw InvalidInput.exception(
            e,
            "Invalid value for the field [%s]. Error message: [%s]",
            e.component,
            e.exception
        );
      }
      catch (IAE e) {
        throw InvalidInput.exception(
            "Invalid parameters supplied to the EXTERN function. Error message: [%s]",
            e.getMessage()
        );
      }
    }
  }

  private static class ArgumentAndException extends Throwable
  {
    private final String component;
    private final Exception exception;

    public ArgumentAndException(String component, Exception exception)
    {
      this.component = component;
      this.exception = exception;
    }
  }

  @Inject
  public ExternalOperatorConversion(@Json final ObjectMapper jsonMapper)
  {
    super(FUNCTION_NAME, new ExternFunction(), jsonMapper);
  }
}
