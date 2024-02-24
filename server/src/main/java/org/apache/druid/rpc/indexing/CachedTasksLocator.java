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

package org.apache.druid.rpc.indexing;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.TasksLocator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CachedTasksLocator implements TasksLocator
{
  private static final String BASE_PATH = "/druid/worker/v1/chat";
  private static final long LOCATION_CACHE_MS = 30_000;

  private final Set<String> trackedTaskIds;
  private final OverlordClient overlordClient;

  // Safe to use HashMap since this is guarded by the lock
  @GuardedBy("this")
  private final Map<String, TaskState> lastKnownStates = new HashMap<>();

  private final Map<String, ServiceLocation> lastKnownLocations = new HashMap<>();
  private boolean closed = false;
  private long lastUpdateTime = -1;
  private SettableFuture<ServiceLocations> pendingFuture = null;


  public CachedTasksLocator(Set<String> trackedTaskIds, OverlordClient overlordClient)
  {
    this.trackedTaskIds = Collections.unmodifiableSet(trackedTaskIds);
    this.overlordClient = overlordClient;
    this.trackedTaskIds.forEach(taskId -> {
      lastKnownStates.put(taskId, TaskState.RUNNING);
      lastKnownLocations.put(taskId, null);
    });
  }

  @Override
  public ListenableFuture<ServiceLocations> locate(String taskId)
  {
    if (!trackedTaskIds.contains(taskId)) {
      throw DruidException.defensive("Provided taskId [%s] is not being tracked by the task locator");
    }
    synchronized (this) {
      if (pendingFuture != null) {
        return Futures.nonCancellationPropagating(pendingFuture);
      } else if (closed || lastKnownStates.get(taskId) != TaskState.RUNNING) {
        return Futures.immediateFuture(ServiceLocations.closed());
      } else if (lastKnownStates.get(taskId) == null || lastUpdateTime + LOCATION_CACHE_MS < System.currentTimeMillis()) {
        final ListenableFuture<Map<String, TaskStatus>> taskStatusesFuture;

        try {
          taskStatusesFuture = overlordClient.taskStatuses(trackedTaskIds);
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }

        // Use shared future for concurrent calls to "locate"; don't want multiple calls out to the Overlord at once.
        // Alias pendingFuture to retVal in case taskStatusFuture is already resolved. (This will make the callback
        // below execute immediately, firing and nulling out pendingFuture.)
        final SettableFuture<ServiceLocations> retVal = (pendingFuture = SettableFuture.create());
        pendingFuture.addListener(
            () -> {
              if (!taskStatusesFuture.isDone()) {
                // pendingFuture may resolve without taskStatusFuture due to close().
                taskStatusesFuture.cancel(true);
              }
            },
            Execs.directExecutor()
        );

        Futures.addCallback(
            taskStatusesFuture,
            new FutureCallback<Map<String, TaskStatus>>()
            {
              @Override
              public void onSuccess(Map<String, TaskStatus> taskStatusMap)
              {
                synchronized (this) {
                  if (pendingFuture != null) {
                    lastUpdateTime = System.currentTimeMillis();

                    // final TaskStatusPlus statusPlus = taskStatus.getStatus();
                    if (taskStatusMap == null) {
                      closed = true;
                      // return;
                    } else {
                      lastKnownLocations.replaceAll((mapTaskId, mapTaskLocation) -> {
                        TaskLocation location = taskStatusMap.get(mapTaskId).getLocation();
                        return new ServiceLocation(
                            location.getHost(),
                            location.getPort(),
                            location.getTlsPort(),
                            StringUtils.format("%s/%s", BASE_PATH, StringUtils.urlEncode(taskId))
                        );
                      });
                      lastKnownStates.replaceAll((mapTaskId, mapTaskState) -> {
                        if (mapTaskState == null) {
                          return null;
                        }
                        TaskStatus fetchedTaskStatus = taskStatusMap.get(taskId);
                        if (fetchedTaskStatus == null) {
                          return null;
                        }
                        return fetchedTaskStatus.getStatusCode();
                      });
                    }



                    if (taskStatusMap == null) {
                      // If the task status is unknown, we'll treat it as closed.
                      lastKnownState = null;
                      lastKnownLocation = null;
                    } else {
                      lastKnownState = statusPlus.getStatusCode();

                      if (TaskLocation.unknown().equals(statusPlus.getLocation())) {
                        lastKnownLocation = null;
                      } else {
                        lastKnownLocation = new ServiceLocation(
                            statusPlus.getLocation().getHost(),
                            statusPlus.getLocation().getPort(),
                            statusPlus.getLocation().getTlsPort(),
                            StringUtils.format("%s/%s", BASE_PATH, StringUtils.urlEncode(taskId))
                        );
                      }
                    }

                    if (lastKnownState != TaskState.RUNNING) {
                      pendingFuture.set(ServiceLocations.closed());
                    } else if (lastKnownLocation == null) {
                      pendingFuture.set(ServiceLocations.forLocations(Collections.emptySet()));
                    } else {
                      pendingFuture.set(ServiceLocations.forLocation(lastKnownLocation));
                    }

                    // Clear pendingFuture once it has been set.
                    pendingFuture = null;
                  }
                }
              }

              @Override
              public void onFailure(Throwable throwable)
              {
                synchronized (this) {
                  if (pendingFuture != null) {
                    pendingFuture.setException(throwable);

                    // Clear pendingFuture once it has been set.
                    pendingFuture = null;
                  }
                }
              }
            },
            MoreExecutors.directExecutor()
        );

        return Futures.nonCancellationPropagating(
            Futures.transform(

            )
        );

        Futures.addCallback(
            taskStatusesFuture,
            new FutureCallback<TaskStatusResponse>()
            {
              @Override
              public void onSuccess(final TaskStatusResponse taskStatus)
              {
                synchronized (this) {
                  if (pendingFuture != null) {
                    lastUpdateTime = System.currentTimeMillis();

                    final TaskStatusPlus statusPlus = taskStatus.getStatus();

                    if (statusPlus == null) {
                      // If the task status is unknown, we'll treat it as closed.
                      lastKnownState = null;
                      lastKnownLocation = null;
                    } else {
                      lastKnownState = statusPlus.getStatusCode();

                      if (TaskLocation.unknown().equals(statusPlus.getLocation())) {
                        lastKnownLocation = null;
                      } else {
                        lastKnownLocation = new ServiceLocation(
                            statusPlus.getLocation().getHost(),
                            statusPlus.getLocation().getPort(),
                            statusPlus.getLocation().getTlsPort(),
                            StringUtils.format("%s/%s", BASE_PATH, StringUtils.urlEncode(taskId))
                        );
                      }
                    }

                    if (lastKnownState != TaskState.RUNNING) {
                      pendingFuture.set(ServiceLocations.closed());
                    } else if (lastKnownLocation == null) {
                      pendingFuture.set(ServiceLocations.forLocations(Collections.emptySet()));
                    } else {
                      pendingFuture.set(ServiceLocations.forLocation(lastKnownLocation));
                    }

                    // Clear pendingFuture once it has been set.
                    pendingFuture = null;
                  }
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                synchronized (lock) {
                  if (pendingFuture != null) {
                    pendingFuture.setException(t);

                    // Clear pendingFuture once it has been set.
                    pendingFuture = null;
                  }
                }
              }
            },
            MoreExecutors.directExecutor()
        );

        return Futures.nonCancellationPropagating(retVal);
      } else {
        return Futures.immediateFuture(ServiceLocations.forLocation(lastKnownLocation));
      }
    }
  }

  @Override
  public void close()
  {

  }
}
