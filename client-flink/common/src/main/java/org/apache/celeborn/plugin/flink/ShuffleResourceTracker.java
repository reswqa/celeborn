/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.plugin.flink;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.listener.WorkerStatusListener;
import org.apache.celeborn.client.listener.WorkersStatus;
import org.apache.celeborn.client.recover.DummyOperationLogManager;
import org.apache.celeborn.client.recover.OperationLogManager;
import org.apache.celeborn.client.recover.Restoreable;
import org.apache.celeborn.client.recover.operationlog.OperationLog;
import org.apache.celeborn.common.meta.ShufflePartitionLocationInfo;
import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.plugin.flink.recover.operationlog.AddPartitionOperationLog;
import org.apache.celeborn.plugin.flink.recover.operationlog.RemovePartitionOperationLog;
import org.apache.celeborn.plugin.flink.recover.operationlog.UnregisterJobOperationLog;

public class ShuffleResourceTracker implements WorkerStatusListener, Restoreable {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleResourceTracker.class);
  private final ExecutorService executorService;
  private final LifecycleManager lifecycleManager;
  // JobID -> ShuffleResourceListener
  private final Map<JobID, JobShuffleResourceListener> shuffleResourceListeners =
      JavaUtils.newConcurrentHashMap();
  private static final int MAX_RETRY_TIMES = 3;
  private OperationLogManager operationLogManager;

  public ShuffleResourceTracker(
      ExecutorService executorService, LifecycleManager lifecycleManager) {
    this(executorService, lifecycleManager, new DummyOperationLogManager());
  }

  public ShuffleResourceTracker(
      ExecutorService executorService,
      LifecycleManager lifecycleManager,
      OperationLogManager operationLogManager) {
    this.executorService = executorService;
    this.lifecycleManager = lifecycleManager;
    this.operationLogManager = operationLogManager;
    lifecycleManager.registerWorkerStatusListener(this);
  }

  public void registerJob(JobShuffleContext jobShuffleContext) {
    if (shuffleResourceListeners.get(jobShuffleContext.getJobId()) != null) {
      shuffleResourceListeners.get(jobShuffleContext.getJobId()).setContext(jobShuffleContext);
    } else {
      shuffleResourceListeners.put(
          jobShuffleContext.getJobId(),
          new JobShuffleResourceListener(jobShuffleContext, executorService, operationLogManager));
    }
  }

  public void addPartitionResource(
      JobID jobId,
      int shuffleId,
      int partitionId,
      ResultPartitionID resultPartitionID,
      ShuffleDescriptor shuffleDescriptor) {
    JobShuffleResourceListener shuffleResourceListener = shuffleResourceListeners.get(jobId);
    shuffleResourceListener.addPartitionResource(
        new AddPartitionOperationLog(
            jobId, shuffleId, partitionId, resultPartitionID, shuffleDescriptor));
  }

  public void removePartitionResource(JobID jobID, int shuffleId, int partitionId) {
    JobShuffleResourceListener shuffleResourceListener = shuffleResourceListeners.get(jobID);
    if (shuffleResourceListener != null) {
      shuffleResourceListener.removePartitionResource(
          new RemovePartitionOperationLog(jobID, shuffleId, partitionId));
    }
  }

  public JobShuffleResourceListener getJobResourceListener(JobID jobID) {
    return shuffleResourceListeners.get(jobID);
  }

  public void unRegisterJob(JobID jobID) {
    shuffleResourceListeners.remove(jobID);
  }

  @Override
  public void notifyChangedWorkersStatus(WorkersStatus workersStatus) {
    try {
      List<WorkerInfo> unknownWorkers = workersStatus.unknownWorkers;
      if (unknownWorkers != null && !unknownWorkers.isEmpty()) {
        // untrack by job
        for (Map.Entry<JobID, JobShuffleResourceListener> entry :
            shuffleResourceListeners.entrySet()) {
          Set<ResultPartitionID> partitionIds = new HashSet<>();
          JobShuffleResourceListener shuffleResourceListener = entry.getValue();
          for (Map.Entry<Integer, Map<Integer, ResultPartitionID>> mapEntry :
              shuffleResourceListener.getResultPartitionMap().entrySet()) {
            int shuffleId = mapEntry.getKey();
            if (!mapEntry.getValue().isEmpty()) {
              for (WorkerInfo unknownWorker : unknownWorkers) {
                Map<WorkerInfo, ShufflePartitionLocationInfo> shuffleAllocateInfo =
                    lifecycleManager.workerSnapshots(shuffleId);
                // shuffleResourceListener may release when the shuffle is ended
                if (shuffleAllocateInfo != null) {
                  ShufflePartitionLocationInfo shufflePartitionLocationInfo =
                      shuffleAllocateInfo.get(unknownWorker);
                  if (shufflePartitionLocationInfo != null) {
                    // TODO if we support partition replica for map partition we need refactor this
                    //  Currently we only untrack primary partitions for map partition
                    shufflePartitionLocationInfo
                        .removeAndGetAllPrimaryPartitionIds()
                        .forEach(
                            id -> {
                              ResultPartitionID resultPartitionId =
                                  shuffleResourceListener.removePartitionResource(
                                      new RemovePartitionOperationLog(
                                          entry.getKey(), shuffleId, id));
                              if (resultPartitionId != null) {
                                partitionIds.add(resultPartitionId);
                              }
                            });
                  }
                }
              }
            }
          }

          shuffleResourceListener.notifyStopTrackingPartitions(partitionIds, MAX_RETRY_TIMES);
        }
      }
    } catch (Throwable e) {
      // listener never throw exception
      LOG.error("Failed to handle unknown workers, message: {}.", e.getMessage(), e);
    }
  }

  public Set<Integer> getJobShuffleIds(JobID jobID) {
    return shuffleResourceListeners.get(jobID).getShuffleIds();
  }

  public Set<JobID> getJobs() {
    return shuffleResourceListeners.keySet();
  }

  @Override
  public void replay(OperationLog operationLog) {
    if (operationLog.getType() == OperationLog.Type.ADD_PARTITION) {
      AddPartitionOperationLog addPartitionOperationLog = (AddPartitionOperationLog) operationLog;

      if (shuffleResourceListeners.get(addPartitionOperationLog.getJobId()) == null) {
        // the jobShuffleContext will be set later, in ShuffleResourceTracker#registerJob
        shuffleResourceListeners.put(
            addPartitionOperationLog.getJobId(),
            new JobShuffleResourceListener(null, executorService, operationLogManager));
      }

      shuffleResourceListeners
          .get(addPartitionOperationLog.getJobId())
          .addPartitionResource(addPartitionOperationLog);
    } else if (operationLog.getType() == OperationLog.Type.REMOVE_PARTITION) {
      RemovePartitionOperationLog removePartitionOperationLog =
          (RemovePartitionOperationLog) operationLog;
      shuffleResourceListeners
          .get(removePartitionOperationLog.getJobId())
          .removePartitionResource(removePartitionOperationLog);
    } else if (operationLog.getType() == OperationLog.Type.UNREGISTER_JOB) {
      UnregisterJobOperationLog unregisterJobOperationLog =
          (UnregisterJobOperationLog) operationLog;
      shuffleResourceListeners.remove(unregisterJobOperationLog.getJobID());
    }
  }

  public static class JobShuffleResourceListener {

    private JobShuffleContext context;
    private final ExecutorService executorService;
    // celeborn shuffleId -> partitionId -> Flink ResultPartitionID
    private Map<Integer, Map<Integer, ResultPartitionID>> resultPartitionMap =
        JavaUtils.newConcurrentHashMap();
    // celeborn ResultPartitionID -> Flink ShuffleDescriptor, used in job recover
    private Map<ResultPartitionID, ShuffleDescriptor> resultPartitionShuffleDescriptorMap =
        JavaUtils.newConcurrentHashMap();

    private OperationLogManager operationLogManager;

    public JobShuffleResourceListener(
        JobShuffleContext jobShuffleContext,
        ExecutorService executorService,
        OperationLogManager operationLogManager) {
      this.context = jobShuffleContext;
      this.executorService = executorService;
      this.operationLogManager = operationLogManager;
    }

    public void addPartitionResource(AddPartitionOperationLog operationLog) {
      operationLogManager.writeOperationLog(operationLog);
      Map<Integer, ResultPartitionID> shufflePartitionMap =
          resultPartitionMap.computeIfAbsent(
              operationLog.getShuffleId(), (s) -> JavaUtils.newConcurrentHashMap());
      shufflePartitionMap.put(operationLog.getPartitionId(), operationLog.getResultPartitionID());
      resultPartitionShuffleDescriptorMap.put(
          operationLog.getResultPartitionID(), operationLog.getShuffleDescriptor());
    }

    public void setContext(JobShuffleContext context) {
      this.context = context;
    }

    private void notifyStopTrackingPartitions(
        Set<ResultPartitionID> partitionIDS, int remainingRetries) {
      if (partitionIDS == null || partitionIDS.isEmpty()) {
        return;
      }

      LOG.info(
          "jobId: {}, stop tracking partitions {}.",
          context.getJobId(),
          Arrays.toString(partitionIDS.toArray()));

      int count = remainingRetries - 1;
      try {
        CompletableFuture<?> future = context.stopTrackingAndReleasePartitions(partitionIDS);
        future.whenCompleteAsync(
            (ignored, throwable) -> {
              if (throwable == null) {
                return;
              }

              if (count == 0) {
                LOG.error(
                    "jobId: {}, Failed to stop tracking partitions {}.",
                    context.getJobId(),
                    Arrays.toString(partitionIDS.toArray()));
                return;
              }
              notifyStopTrackingPartitions(partitionIDS, count);
            },
            executorService);
      } catch (Throwable throwable) {
        if (count == 0) {
          LOG.error(
              "jobId: {}, Failed to stop tracking partitions {}.",
              context.getJobId(),
              Arrays.toString(partitionIDS.toArray()),
              throwable);
          return;
        }
        notifyStopTrackingPartitions(partitionIDS, count);
      }
    }

    public Map<Integer, Map<Integer, ResultPartitionID>> getResultPartitionMap() {
      return resultPartitionMap;
    }

    public Map<ResultPartitionID, ShuffleDescriptor> getResultPartitionShuffleDescriptorMap() {
      return resultPartitionShuffleDescriptorMap;
    }

    public ResultPartitionID removePartitionResource(
        RemovePartitionOperationLog removePartitionOperationLog) {
      operationLogManager.writeOperationLog(removePartitionOperationLog);
      Map<Integer, ResultPartitionID> partitionIDMap =
          resultPartitionMap.get(removePartitionOperationLog.getShuffleId());
      ResultPartitionID resultPartitionID = null;
      if (partitionIDMap != null) {
        resultPartitionID = partitionIDMap.remove(removePartitionOperationLog.getPartitionId());
        resultPartitionShuffleDescriptorMap.remove(resultPartitionID);
      }
      return resultPartitionID;
    }

    public Set<Integer> getShuffleIds() {
      return resultPartitionMap.keySet();
    }
  }
}
