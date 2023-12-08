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

package org.apache.druid.query.aggregation;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.serde.cell.RandomStringUtils;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class BackwordCompatibleSerializablePairLongStringSimpleStagedSerdeTest
{
  private static final OlderSerializablePairLongStringSimpleStagedSerde OLDER_SERDE =
      new OlderSerializablePairLongStringSimpleStagedSerde();
  private static final SerializablePairLongStringSimpleStagedSerde SERDE =
      new SerializablePairLongStringSimpleStagedSerde();

  private final RandomStringUtils randomStringUtils = new RandomStringUtils(new Random(0));

  @Test
  public void testSimple()
  {
    testValue(new SerializablePairLongString(Long.MAX_VALUE, "fuu"));
  }

  @Test
  public void testNull()
  {
    testValue(null);
  }

  @Test
  public void testNullString()
  {
    SerializablePairLongString value = new SerializablePairLongString(Long.MAX_VALUE, null);
    // Write using the older serde, read using the newer serde
    Assert.assertEquals(
        new SerializablePairLongString(Long.MAX_VALUE, ""),
        readUsingSerde(writeUsingSerde(value, OLDER_SERDE), SERDE)
    );
    // Write using the newer serde, read using the older serde
    Assert.assertEquals(
        new SerializablePairLongString(Long.MAX_VALUE, null),
        readUsingSerde(writeUsingSerde(value, SERDE), OLDER_SERDE)
    );
    // Compare the length of the serialized bytes for the value
    Assert.assertEquals(writeUsingSerde(value, OLDER_SERDE).length, writeUsingSerde(value, SERDE).length);
  }

  @Test
  public void testEmptyString()
  {
    SerializablePairLongString value = new SerializablePairLongString(Long.MAX_VALUE, "");
    // Write using the older serde, read using the newer serde
    Assert.assertEquals(
        new SerializablePairLongString(Long.MAX_VALUE, ""),
        readUsingSerde(writeUsingSerde(value, OLDER_SERDE), SERDE)
    );
    // Write using the newer serde, read using the older serde
    Assert.assertEquals(
        new SerializablePairLongString(Long.MAX_VALUE, null),
        readUsingSerde(writeUsingSerde(value, SERDE), OLDER_SERDE)
    );
    // Compare the length of the serialized bytes for the value
    Assert.assertEquals(writeUsingSerde(value, OLDER_SERDE).length, writeUsingSerde(value, SERDE).length);

  }

  @Test
  public void testLargeString()
  {
    testValue(new SerializablePairLongString(Long.MAX_VALUE, randomStringUtils.randomAlphanumeric(1024 * 1024)));
  }

  private void testValue(@Nullable SerializablePairLongString value)
  {
    // Write using the older serde, read using the newer serde
    Assert.assertEquals(
        value,
        readUsingSerde(writeUsingSerde(value, OLDER_SERDE), SERDE)
    );
    // Write using the newer serde, read using the older serde
    Assert.assertEquals(
        value,
        readUsingSerde(writeUsingSerde(value, SERDE), OLDER_SERDE)
    );
    // Compare the length of the serialized bytes for the value
    Assert.assertEquals(writeUsingSerde(value, OLDER_SERDE).length, writeUsingSerde(value, SERDE).length);
  }

  private static byte[] writeUsingSerde(
      @Nullable SerializablePairLongString value,
      StagedSerde<SerializablePairLongString> serde
  )
  {
    return serde.serialize(value);
  }

  private static SerializablePairLongString readUsingSerde(
      byte[] bytes,
      StagedSerde<SerializablePairLongString> serde
  )
  {
    return serde.deserialize(bytes);
  }

  /**
   * Older serde class for simple long-string pair serde, that treated empty and null strings equivalently and returned
   * coerced both to null
   */
  private static class OlderSerializablePairLongStringSimpleStagedSerde
      implements StagedSerde<SerializablePairLongString>
  {
    @Override
    public StorableBuffer serializeDelayed(@Nullable SerializablePairLongString value)
    {
      if (value == null) {
        return StorableBuffer.EMPTY;
      }

      String rhsString = value.rhs;
      byte[] rhsBytes = StringUtils.toUtf8WithNullToEmpty(rhsString);

      return new StorableBuffer()
      {
        @Override
        public void store(ByteBuffer byteBuffer)
        {
          Preconditions.checkNotNull(value.lhs, "Long in SerializablePairLongString must be non-null");

          byteBuffer.putLong(value.lhs);
          byteBuffer.putInt(rhsBytes.length);

          if (rhsBytes.length > 0) {
            byteBuffer.put(rhsBytes);
          }
        }

        @Override
        public int getSerializedSize()
        {
          return Long.BYTES + Integer.BYTES + rhsBytes.length;
        }
      };
    }

    @Nullable
    @Override
    public SerializablePairLongString deserialize(ByteBuffer byteBuffer)
    {
      if (byteBuffer.remaining() == 0) {
        return null;
      }

      ByteBuffer readOnlyBuffer = byteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
      long lhs = readOnlyBuffer.getLong();
      int stringSize = readOnlyBuffer.getInt();
      String lastString = null;

      if (stringSize > 0) {
        byte[] stringBytes = new byte[stringSize];

        readOnlyBuffer.get(stringBytes, 0, stringSize);
        lastString = StringUtils.fromUtf8(stringBytes);
      }

      return new SerializablePairLongString(lhs, lastString);
    }
  }

}
