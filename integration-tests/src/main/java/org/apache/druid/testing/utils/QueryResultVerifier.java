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

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryResultVerifier
{
  private static final Logger LOG = new Logger(QueryResultVerifier.class);

  public static boolean compareResults(
      Iterable<Map<String, Object>> actual,
      Iterable<Map<String, Object>> expected,
      List<String> fieldsToTest
  )
  {
    Iterator<Map<String, Object>> actualIter = actual.iterator();
    Iterator<Map<String, Object>> expectedIter = expected.iterator();

    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Map<String, Object> actualRes = actualIter.next();
      Map<String, Object> expRes = expectedIter.next();

      if (fieldsToTest != null && !fieldsToTest.isEmpty()) {
        for (String field : fieldsToTest) {
          if (!actualRes.get(field).equals(expRes.get(field))) {
            LOG.info(StringUtils.format(
                "Field [%s] mismatch. Expected: [%s], Actual: [%s]",
                field,
                expRes,
                actualRes
            ));
            return false;
          }
        }
      } else {
        if (!actualRes.equals(expRes)) {
          LOG.info(StringUtils.format(
              "Row mismatch. Expected: [%s], Actual: [%s]",
              expRes,
              actualRes
          ));
          return false;
        }
      }
    }

    if (actualIter.hasNext() || expectedIter.hasNext()) {
      LOG.info("Results size mismatch");
      return false;
    }
    return true;
  }
}
