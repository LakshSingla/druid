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

package org.apache.druid.segment.join;

import org.apache.druid.query.DataSource;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.InlineDataSource;

import java.util.Optional;

/**
 * Creates a joinable from the {@link FrameBasedInlineDataSource}. This materializes the datasource to an
 * {@link InlineDataSource}, before creating the joinable on it, which carries the overhead of this conversion.
 */
public class FrameBasedInlineJoinableFactory implements JoinableFactory
{
  private final InlineJoinableFactory INLINE_JOINABLE_FACTORY = new InlineJoinableFactory();


  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    return dataSource instanceof FrameBasedInlineDataSource;
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    FrameBasedInlineDataSource frameBasedInlineDataSource = (FrameBasedInlineDataSource) dataSource;
    InlineDataSource inlineDataSource = frameBasedInlineDataSource.toInlineDataSource();

    return INLINE_JOINABLE_FACTORY.build(inlineDataSource, condition);
  }
}
