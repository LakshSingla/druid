package org.apache.druid.rpc;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

public interface TasksLocator extends Closeable
{
  ListenableFuture<ServiceLocations> locate(String taskId);

  @Override
  void close();
}
