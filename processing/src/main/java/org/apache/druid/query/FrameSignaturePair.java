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
import com.google.common.base.Preconditions;
import org.apache.druid.frame.Frame;
import org.apache.druid.segment.column.RowSignature;

public class FrameSignaturePair
{
  final Frame frame;
  final RowSignature rowSignature;

  @JsonCreator
  public FrameSignaturePair(
      @JsonProperty("frame") Frame frame,
      @JsonProperty("rowSignature") RowSignature rowSignature)
  {
    this.frame = Preconditions.checkNotNull(frame, "'frame' must be non null");
    this.rowSignature = Preconditions.checkNotNull(rowSignature, "'rowSignature' must be non null");
  }

  @JsonProperty("frame")
  public Frame getFrame()
  {
    return frame;
  }

  @JsonProperty("rowSignature")
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }
}
