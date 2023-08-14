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

package org.apache.druid.server;

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

public class SubqueryLimitUtils
{
  private static final double SUBQUERY_MEMORY_BYTES_FRACTION = 0.5;
  private static final Logger log = new Logger(SubqueryLimitUtils.class);


  public static final String UNLIMITED_LIMIT_VALUE = "unlimited";
  public static final String AUTO_LIMIT_VALUE = "auto";

  public static final Long UNLIMITED_LIMIT_REPRESENTATION = -1L;
  public static final Long AUTO_LIMIT_REPRESENTATION = -2L;

  private final long autoLimitBytes;

  public SubqueryLimitUtils(final LookupExtractorFactoryContainerProvider lookupManager, final int brokerNumHttpConnections)
  {
    autoLimitBytes = computeLimitBytesForAuto(lookupManager, brokerNumHttpConnections);
  }

  public long convertSubqueryLimitStringToLong(final String maxSubqueryLimit)
  {
    if (UNLIMITED_LIMIT_VALUE.equalsIgnoreCase(maxSubqueryLimit)) {
      return UNLIMITED_LIMIT_REPRESENTATION;
    }
    if (AUTO_LIMIT_VALUE.equalsIgnoreCase(maxSubqueryLimit)) {
      return autoLimitBytes;
    }

    long retVal;
    try {
      retVal = Long.parseLong(maxSubqueryLimit);
    }
    catch (NumberFormatException e) {
      throw InvalidInput.exception(
          e,
          "Unable to parse the provided maxSubqueryLimit [%s] to a valid number. Valid values for the "
          + "maxSubqueryLimits can be 'auto', 'unlimited' or a positive number representing bytes to reserve.",
          maxSubqueryLimit
      );
    }

    // This can happen if the provided number is greater than Longs.MAX_VALUE
    if (retVal < 0) {
      throw InvalidInput.exception("Limit too large");
    }

    return retVal;
  }

  private static long computeLimitBytesForAuto(
      final LookupExtractorFactoryContainerProvider lookupManager,
      final int brokerNumHttpConnections
  )
  {
    final long maxMemoryInJvm = Runtime.getRuntime().maxMemory();
    return ((long) (maxMemoryInJvm * SUBQUERY_MEMORY_BYTES_FRACTION) - computeLookupFootprint(lookupManager))
           / brokerNumHttpConnections;
  }

  private static long computeLookupFootprint(final LookupExtractorFactoryContainerProvider lookupManager)
  {

    if (lookupManager == null || lookupManager.getAllLookupNames() == null) {
      log.warn("Failed to get the lookupManager for estimating lookup size. Skipping.");
      return 0;
    }

    int lookupCount = 0;
    long lookupFootprint = 0;

    for (final String lookupName : lookupManager.getAllLookupNames()) {
      final LookupExtractorFactoryContainer container = lookupManager.get(lookupName).orElse(null);

      if (container != null) {
        try {
          final LookupExtractor extractor = container.getLookupExtractorFactory().get();
          lookupFootprint += extractor.estimateHeapFootprint();
          lookupCount++;
        }
        catch (Exception e) {
          log.noStackTrace().warn(e, "Failed to load lookup [%s] for size estimation. Skipping.", lookupName);
        }
      }
    }

    log.debug("Lookup footprint: [%d] lookups with [%,d] total bytes.", lookupCount, lookupFootprint);

    return lookupFootprint;
  }
}
