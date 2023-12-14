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

package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ValueType;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ArrayLongGroupByColumnSelectorStrategy extends ArrayNumericGroupByColumnSelectorStrategy<Long>
{
  public ArrayLongGroupByColumnSelectorStrategy()
  {
    super(Long.BYTES);
  }

  @VisibleForTesting
  ArrayLongGroupByColumnSelectorStrategy(
      List<List<Long>> dictionary,
      Object2IntOpenHashMap<List<Long>> reverseDictionary
  )
  {
    super(dictionary, reverseDictionary, Long.BYTES);
  }

  @Override
  protected int computeDictionaryId(ColumnValueSelector selector)
  {
    Object object = selector.getObject();
    if (object == null) {
      return GROUP_BY_MISSING_VALUE;
    } else if (object instanceof Long) {
      return addToIndexedDictionary(ImmutableList.of((Long) object));
    } else if (object instanceof List) {
      return addToIndexedDictionary((List<Long>) object);
    } else if (object instanceof Long[]) {
      return addToIndexedDictionary(Arrays.asList((Long[]) object));
    } else if (object instanceof Object[]) {
      return addToIndexedDictionary(Arrays.stream(((Object[]) (object)))
                                          .map(a -> (Long) a)
                                          .collect(Collectors.toList()));
    } else {
      throw new ISE("Found unexpected object type [%s] in %s array.", object.getClass().getName(), ValueType.LONG);
    }
  }
}
