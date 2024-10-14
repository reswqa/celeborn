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

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.recover.RecoverableStore;
import org.apache.celeborn.client.recover.operationlog.OperationLog;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.plugin.flink.recover.RecoverableStoreFactory;
import org.apache.celeborn.plugin.flink.recover.RecoverableStoreShuffleContext;
import org.apache.celeborn.plugin.flink.recover.operationlog.AppRegisterOperationLog;
import org.apache.celeborn.plugin.flink.recover.operationlog.UnregisterJobOperationLog;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

public class RemoteShuffleMasterDelegation implements RecoverableStoreShuffleContext {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMasterDelegation.class);
  private final ShuffleMasterContext shuffleMasterContext;
  private boolean jobRecoveryEnabled;
  private final boolean isSessionMode;
  private String celebornAppId;
  private volatile LifecycleManager lifecycleManager;
  private ShuffleTaskInfo shuffleTaskInfo;
  private ShuffleResourceTracker shuffleResourceTracker;
  private final ScheduledExecutorService executor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
          "celeborn-client-remote-shuffle-master-executor");
  private final ResultPartitionAdapter resultPartitionDelegation;
  private final long lifecycleManagerTimestamp;
  private CelebornConf celebornConf;
  private RecoverableStore recoverableStore;
  // jobId to time of job marked to be expired
  // The job included in this collection is not expire immediately, it will be released when the
  // time is greater JOB_EXPIRE_TIME_IN_MS than the mark time
  private Map<JobID, Long> expiredJobIds = JavaUtils.newConcurrentHashMap();
  private static final long JOB_EXPIRE_TIME_IN_MS = 300 * 1000;

  public RemoteShuffleMasterDelegation(
      ShuffleMasterContext shuffleMasterContext, ResultPartitionAdapter resultPartitionDelegation) {
    checkShuffleConfig(shuffleMasterContext.getConfiguration());
    this.shuffleMasterContext = shuffleMasterContext;
    this.resultPartitionDelegation = resultPartitionDelegation;
    this.lifecycleManagerTimestamp = System.currentTimeMillis();
    this.celebornConf = FlinkUtils.toCelebornConf(shuffleMasterContext.getConfiguration());
    // if not set, set to true as default for flink
    celebornConf.setIfMissing(CelebornConf.CLIENT_CHECKED_USE_ALLOCATED_WORKERS(), true);
    if (celebornConf.clientPushReplicateEnabled()) {
      shuffleMasterContext.onFatalError(
          new RuntimeException("Currently replicate shuffle data is unsupported for flink."));
    }

    this.isSessionMode = FlinkUtils.isSessionMode(shuffleMasterContext.getConfiguration());
    this.jobRecoveryEnabled =
        FlinkUtils.jobRecoveryEnabled(shuffleMasterContext.getConfiguration());
    if (isSessionMode) {
      this.jobRecoveryEnabled =
          this.jobRecoveryEnabled && celebornConf.recoveryFlinkJobInSessionModeEnabled();
    }

    LOG.debug("shuffleMasterContext: {}", shuffleMasterContext.getConfiguration());
  }

  public ShuffleResourceTracker getShuffleResourceTracker() {
    return shuffleResourceTracker;
  }

  public void registerJob(JobShuffleContext context) {
    JobID jobID = context.getJobId();
    try {
      if (lifecycleManager == null) {
        synchronized (RemoteShuffleMasterDelegation.class) {
          if (lifecycleManager == null) {
            recover(jobID);
          }
        }
      }
    } catch (Exception e) {
      shuffleMasterContext.onFatalError(
          new RuntimeException("Can not recover from operation log.", e));
    }

    if (expiredJobIds.get(jobID) != null) {
      expiredJobIds.remove(jobID);
    }

    LOG.info("Register job: {}.", jobID);
    shuffleResourceTracker.registerJob(context);
  }

  public void unregisterJob(JobID jobID) {
    LOG.info("Unregister job: {}.", jobID);
    Set<Integer> shuffleIds = shuffleResourceTracker.getJobShuffleIds(jobID);
    if (shuffleIds != null) {
      executor.execute(
          () -> {
            try {
              expireJob(jobID, !jobRecoveryEnabled);
            } catch (Throwable throwable) {
              LOG.error("Encounter an error when unregistering job: {}.", jobID, throwable);
            }
          });
    }
  }

  public CompletableFuture<RemoteShuffleDescriptor> registerPartitionWithProducer(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    return CompletableFuture.supplyAsync(
        () -> {
          FlinkResultPartitionInfo resultPartitionInfo =
              new FlinkResultPartitionInfo(jobID, partitionDescriptor, producerDescriptor);
          ShuffleResourceDescriptor shuffleResourceDescriptor =
              shuffleTaskInfo.genShuffleResourceDescriptor(
                  resultPartitionInfo.getShuffleId(),
                  resultPartitionInfo.getTaskId(),
                  resultPartitionInfo.getAttemptId());

          RemoteShuffleResource remoteShuffleResource =
              new RemoteShuffleResource(
                  lifecycleManager.getHost(),
                  lifecycleManager.getPort(),
                  lifecycleManagerTimestamp,
                  shuffleResourceDescriptor);

          RemoteShuffleDescriptor shuffleDescriptor =
              new RemoteShuffleDescriptor(
                  celebornAppId,
                  jobID,
                  resultPartitionInfo.getShuffleId(),
                  partitionDescriptor.getNumberOfSubpartitions(),
                  resultPartitionInfo.getResultPartitionId(),
                  remoteShuffleResource);

          shuffleResourceTracker.addPartitionResource(
              jobID,
              shuffleResourceDescriptor.getShuffleId(),
              shuffleResourceDescriptor.getPartitionId(),
              resultPartitionInfo.getResultPartitionId(),
              shuffleDescriptor);

          return shuffleDescriptor;
        },
        executor);
  }

  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
    executor.execute(
        () -> {
          if (!(shuffleDescriptor instanceof RemoteShuffleDescriptor)) {
            LOG.error(
                "Only RemoteShuffleDescriptor is supported {}.",
                shuffleDescriptor.getClass().getName());
            shuffleMasterContext.onFatalError(
                new RuntimeException("Illegal shuffle descriptor type."));
            return;
          }
          try {
            RemoteShuffleDescriptor descriptor = (RemoteShuffleDescriptor) shuffleDescriptor;
            RemoteShuffleResource shuffleResource = descriptor.getShuffleResource();
            ShuffleResourceDescriptor resourceDescriptor =
                shuffleResource.getMapPartitionShuffleDescriptor();
            LOG.debug("release partition resource: {}.", resourceDescriptor);
            lifecycleManager.releasePartition(
                resourceDescriptor.getShuffleId(), resourceDescriptor.getPartitionId());
            shuffleResourceTracker.removePartitionResource(
                descriptor.getJobId(),
                resourceDescriptor.getShuffleId(),
                resourceDescriptor.getPartitionId());
          } catch (Throwable throwable) {
            // it is not a problem if we failed to release the target data partition
            // because the session timeout mechanism will do the work for us latter
            LOG.debug("Failed to release data partition {}.", shuffleDescriptor, throwable);
          }
        });
  }

  public MemorySize computeShuffleMemorySizeForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    for (ResultPartitionType partitionType :
        taskInputsOutputsDescriptor.getPartitionTypes().values()) {
      boolean isBlockingShuffle =
          resultPartitionDelegation.isBlockingResultPartition(partitionType);
      if (!isBlockingShuffle) {
        throw new RuntimeException(
            "Blocking result partition type expected but found " + partitionType);
      }
    }

    int numResultPartitions = taskInputsOutputsDescriptor.getSubpartitionNums().size();
    CelebornConf conf = FlinkUtils.toCelebornConf(shuffleMasterContext.getConfiguration());
    long numBytesPerPartition = conf.clientFlinkMemoryPerResultPartition();
    long numBytesForOutput = numBytesPerPartition * numResultPartitions;

    int numInputGates = taskInputsOutputsDescriptor.getInputChannelNums().size();
    long numBytesPerGate = conf.clientFlinkMemoryPerInputGate();
    long numBytesForInput = numBytesPerGate * numInputGates;

    LOG.debug(
        "Announcing number of bytes {} for output and {} for input.",
        numBytesForOutput,
        numBytesForInput);

    return new MemorySize(numBytesForInput + numBytesForOutput);
  }

  public void close() throws Exception {
    try {
      LifecycleManager manager = lifecycleManager;
      if (null != manager) {
        manager.stop();
      }
      recoverableStore.stop();
    } catch (Exception e) {
      LOG.warn("Encounter exception when shutdown: {}", e.getMessage(), e);
    }

    ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, executor);
  }

  /**
   * Checks the shuffle config given the Flink configuration.
   *
   * <p>The config option {@link ExecutionOptions#BATCH_SHUFFLE_MODE} should configure as {@link
   * BatchShuffleMode#ALL_EXCHANGES_BLOCKING}.
   *
   * @param configuration The Flink configuration with shuffle config.
   */
  private void checkShuffleConfig(Configuration configuration) {
    if (configuration.get(ExecutionOptions.BATCH_SHUFFLE_MODE)
        != BatchShuffleMode.ALL_EXCHANGES_BLOCKING) {
      throw new IllegalArgumentException(
          String.format(
              "The config option %s should configure as %s",
              ExecutionOptions.BATCH_SHUFFLE_MODE.key(),
              BatchShuffleMode.ALL_EXCHANGES_BLOCKING.name()));
    }
  }

  public boolean hasJobs() {
    return !expiredJobIds.isEmpty() || !shuffleResourceTracker.getJobs().isEmpty();
  }

  private void createLifecycleManager() {
    LOG.info(
        "CelebornAppId: {}, deploy mode: {}, job recovery enable: {}",
        celebornAppId,
        shuffleMasterContext.getConfiguration().get(DeploymentOptions.TARGET),
        jobRecoveryEnabled);
    lifecycleManager = new LifecycleManager(celebornAppId, celebornConf, recoverableStore);
    shuffleResourceTracker =
        new ShuffleResourceTracker(executor, lifecycleManager, recoverableStore);
    lifecycleManager.registerWorkerStatusListener(shuffleResourceTracker);
  }

  void recover(JobID currentJobID) throws IOException {
    String pathId = null;
    if (!isSessionMode) {
      // use fixed jobID path persistent operation log
      pathId = currentJobID.toString();
    }
    recoverableStore =
        RecoverableStoreFactory.createOperationLogStore(
            pathId, shuffleMasterContext.getConfiguration(), this);
    shuffleTaskInfo = new ShuffleTaskInfo(recoverableStore);
    boolean needRecover = true;
    OperationLog operationLog = recoverableStore.readOperation();
    if (operationLog != null) {
      celebornAppId = ((AppRegisterOperationLog) operationLog).getCelebornAppId();
      recoverableStore.registerAppIdOperation(operationLog);
      recoverableStore.setRecoverFinished(false);
    } else {
      needRecover = false;
      celebornAppId = FlinkUtils.toCelebornAppId(lifecycleManagerTimestamp, currentJobID);
      recoverableStore.registerAppIdOperation(new AppRegisterOperationLog(celebornAppId));
      recoverableStore.setRecoverFinished(true);
    }

    createLifecycleManager();
    if (!jobRecoveryEnabled || !needRecover) {
      lifecycleManager.initialize();
      return;
    }

    while ((operationLog = recoverableStore.readOperation()) != null) {
      LOG.debug("Recover operationLog: {}", operationLog);
      if (operationLog instanceof UnregisterJobOperationLog) {
        UnregisterJobOperationLog unregisterJobOperationLog =
            (UnregisterJobOperationLog) operationLog;
        if (!unregisterJobOperationLog.isAlreadyReleased()) {
          expiredJobIds.put(unregisterJobOperationLog.getJobID(), 0L);
        } else {
          expireJob(unregisterJobOperationLog.getJobID(), true);
        }
      } else {
        try {
          shuffleResourceTracker.replay(operationLog);
          shuffleTaskInfo.replay(operationLog);
          lifecycleManager.replay(operationLog);
        } catch (Exception e) {
          LOG.warn(
              "Recover operationLog "
                  + operationLog
                  + " error, may due to in complete operation log, just ignore this.",
              e);
        }
      }
    }

    recoverableStore.setRecoverFinished(true);
    lifecycleManager.initialize();

    try {
      if (isSessionMode) {
        // refresh unregister jobIds expire time
        for (JobID jobID : expiredJobIds.keySet()) {
          expiredJobIds.put(jobID, System.currentTimeMillis());
        }
        // expire with fix rate
        executor.scheduleAtFixedRate(
            () -> {
              long current = System.currentTimeMillis();
              boolean hasExpiredJob = false;
              for (Map.Entry<JobID, Long> entry : expiredJobIds.entrySet()) {
                if (current - entry.getValue() > JOB_EXPIRE_TIME_IN_MS) {
                  expireJob(entry.getKey(), true);
                  hasExpiredJob = true;
                }
              }

              // try clear recoverable store if no job running
              if (hasExpiredJob) {
                recoverableStore.clear();
              }
            },
            180,
            180,
            TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      shuffleMasterContext.onFatalError(
          new RuntimeException("Can not recover from operation log.", e));
    }
  }

  private void expireJob(JobID jobID, boolean expireImmediately) {
    LOG.info("expire flink job: {}, expireImmediately: {}", jobID, expireImmediately);
    if (expireImmediately) {
      Set<Integer> shuffleIds = shuffleResourceTracker.getJobShuffleIds(jobID);
      for (Integer shuffleId : shuffleIds) {
        lifecycleManager.unregisterShuffle(shuffleId);
        shuffleTaskInfo.removeExpiredShuffle(shuffleId);
      }
      shuffleResourceTracker.unRegisterJob(jobID);
      expiredJobIds.remove(jobID);
      recoverableStore.writeOperation(new UnregisterJobOperationLog(jobID, true));
    } else {
      expiredJobIds.put(jobID, System.currentTimeMillis());
      recoverableStore.writeOperation(new UnregisterJobOperationLog(jobID, false));
    }
  }
}
