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

package org.apache.druid.msq.input.external;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.WarningCounters;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class ExternalSegment implements Segment
{

  private final Segment delegate;
  private final InputSource inputSource;

  public ExternalSegment(
      final InputSource inputSource,
      final SegmentId segmentId,
      final InputSourceReader reader,
      final InputStats inputStats,
      final WarningCounters warningCounters,
      final Consumer<Throwable> warningPublisher,
      final boolean incrementCounters,
      final ChannelCounters channelCounters,
      final RowSignature signature
  )
  {
    this.inputSource = inputSource;
    delegate = new RowBasedSegment<>(
        segmentId,
        new BaseSequence<>(
            new BaseSequence.IteratorMaker<InputRow, CloseableIterator<InputRow>>()
            {
              @Override
              public CloseableIterator<InputRow> make()
              {
                try {
                  CloseableIterator<InputRow> baseIterator = reader.read(inputStats);
                  return new CloseableIterator<InputRow>()
                  {
                    private InputRow next = null;

                    @Override
                    public void close() throws IOException
                    {
                      baseIterator.close();
                    }

                    @Override
                    public boolean hasNext()
                    {
                      while (true) {
                        try {
                          while (next == null && baseIterator.hasNext()) {
                            next = baseIterator.next();
                          }
                          break;
                        }
                        catch (ParseException e) {
                          warningCounters.incrementWarningCount(CannotParseExternalDataFault.CODE);
                          warningPublisher.accept(e);
                        }
                      }
                      return next != null;
                    }

                    @Override
                    public InputRow next()
                    {
                      if (!hasNext()) {
                        throw new NoSuchElementException();
                      }
                      final InputRow row = next;
                      next = null;
                      return row;
                    }
                  };
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void cleanup(CloseableIterator<InputRow> iterFromMake)
              {
                try {
                  iterFromMake.close();
                  // We increment the file count whenever the caller calls clean up. So we can double count here
                  // if the callers are not careful.
                  // This logic only works because we are using FilePerSplitHintSpec. Each input source only
                  // has one file.
                  if (incrementCounters) {
                    channelCounters.incrementFileCount();
                    channelCounters.incrementBytes(inputStats.getProcessedBytes());
                  }
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            }
        ),
        RowAdapters.standardRow(),
        signature
    );

  }

  @Override
  public SegmentId getId()
  {
    return delegate.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return delegate.getDataInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return delegate.asQueryableIndex();
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return delegate.asStorageAdapter();
  }

  @Nullable
  @Override
  public <T> T as(@Nonnull Class<T> clazz)
  {
    return delegate.as(clazz);
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }

  public InputSource externalInputSource()
  {
    return inputSource;
  }
}
