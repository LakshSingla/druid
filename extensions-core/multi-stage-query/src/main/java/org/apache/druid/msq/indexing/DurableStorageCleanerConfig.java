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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class DurableStorageCleanerConfig
{

  private static final long DEFAULT_DELAY_SECONDS = 86400L;

  /**
   * Whether the {@link DurableStorageCleaner} helper should be enabled or not
   */
  @JsonProperty
  private final boolean enabled;

  /**
   * The delay (in seconds) after the last run post which the durable storage cleaner would clean the outputs
   */
  @JsonProperty
  private final long delaySeconds;

  @JsonCreator
  public DurableStorageCleanerConfig(
      @JsonProperty("enabled") final Boolean enabled,
      @JsonProperty("delay") final Long delaySeconds
  )
  {
    this.enabled = enabled != null && enabled;
    this.delaySeconds = delaySeconds != null ? delaySeconds : DEFAULT_DELAY_SECONDS;

    Preconditions.checkArgument(this.delaySeconds > 0, "delay must be greater than 0");
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public long getDelaySeconds()
  {
    return delaySeconds;
  }

  @Override
  public String toString()
  {
    return "DurableStorageCleanerConfig{" +
           "enabled=" + enabled +
           ", delaySeconds=" + delaySeconds +
           '}';
  }
}
