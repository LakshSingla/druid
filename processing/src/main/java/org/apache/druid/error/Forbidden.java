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

package org.apache.druid.error;

public class Forbidden extends DruidException.Failure
{

  public static DruidException exception()
  {
    return exception("Unauthorize");
  }

  public static DruidException exception(String msg, Object... args)
  {
    return exception(null, msg, args);
  }

  public static DruidException exception(Throwable t, String msg, Object... args)
  {
    return DruidException.fromFailure(new Forbidden(t, msg, args));
  }

  private final Throwable t;
  private final String msg;
  private final Object[] args;

  private Forbidden(
      Throwable t,
      String msg,
      Object... args
  )
  {
    super("forbidden");
    this.t = t;
    this.msg = msg;
    this.args = args;
  }

  @Override
  public DruidException makeException(DruidException.DruidExceptionBuilder bob)
  {
    bob = bob.forPersona(DruidException.Persona.USER)
             .ofCategory(DruidException.Category.FORBIDDEN);

    if (t == null) {
      return bob.build(msg, args);
    } else {
      return bob.build(t, msg, args);
    }
  }
}
