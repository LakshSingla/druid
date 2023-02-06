/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { sum } from 'd3-array';

import { CapacityInfo } from '../druid-models';
import { Api } from '../singletons';

export async function getClusterCapacity(): Promise<CapacityInfo> {
  const workersResponse = await Api.instance.get('/druid/indexer/v1/workers', {
    timeout: 500,
  });

  const usedTaskSlots = sum(
    workersResponse.data,
    (workerInfo: any) => Number(workerInfo.currCapacityUsed) || 0,
  );

  const totalTaskSlots = sum(workersResponse.data, (workerInfo: any) =>
    Number(workerInfo.worker.capacity),
  );

  return {
    availableTaskSlots: totalTaskSlots - usedTaskSlots,
    usedTaskSlots,
    totalTaskSlots,
  };
}

export async function maybeGetClusterCapacity(): Promise<CapacityInfo | undefined> {
  try {
    return await getClusterCapacity();
  } catch {
    return;
  }
}
