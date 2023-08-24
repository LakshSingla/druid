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

package org.apache.druid.data.input.impl.prefetch;

import com.google.common.base.Predicates;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.java.util.common.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface ObjectOpenFunction<T>
{
  InputStream open(T object) throws IOException;

  default InputStream open(T object, long start) throws IOException
  {
    return open(object);
  }

  default InputEntity.CleanableFile fetchInternal(T object, File tempFile, byte[] fetchBuffer) throws IOException
  {
    FileUtils.copyLarge(
        () -> this.open(object),
        tempFile,
        fetchBuffer,
        Predicates.alwaysFalse(),
        1,
        ""
    );

    return new InputEntity.CleanableFile()
    {
      @Override
      public File file()
      {
        return tempFile;
      }

      @Override
      public void close()
      {
        tempFile.delete();
      }
    };
  }
}
