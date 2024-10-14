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

package org.apache.celeborn.tests.flink;

import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.common.util.JavaUtils;

/** referring to org.apache.flink.test.scheduling.JMFailoverITCase. */
class JobRecoveryHelper {

  public static ExecutorService executor;

  public static final int DEFAULT_MAX_PARALLELISM = 4;
  public static final int SOURCE_PARALLELISM = 8;

  public static final int NUMBER_KEYS = 10000;
  public static final int NUMBER_OF_EACH_KEY = 4;

  public EmbeddedHaServicesWithLeadershipControl highAvailabilityServices;

  public java.nio.file.Path temporaryFolder;

  public int numTaskManagers = 4;

  public int numSlotsPerTaskManager = 4;

  public Configuration flinkConfiguration = new Configuration();

  public MiniCluster flinkCluster;

  public Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier = null;

  void before() throws Exception {
    flinkConfiguration = new Configuration();
    SourceTail.clear();
    StubMapFunction.clear();
    StubRecordSink.clear();
    temporaryFolder = Files.createTempDirectory("temp");
    executor = Executors.newSingleThreadScheduledExecutor();
  }

  void after() {
    Throwable exception = null;

    try {
      if (flinkCluster != null) {
        flinkCluster.close();
      }
      JavaUtils.deleteRecursively(temporaryFolder.toFile());
      executor.shutdown();
      Thread.sleep(5000);
    } catch (Throwable throwable) {
      exception = throwable;
    }

    if (exception != null) {
      ExceptionUtils.rethrow(exception);
    }
  }

  public JobGraph prepareEnvAndGetJobGraph(int celebornMasterPort, String testName)
      throws Exception {
    Configuration configuration = new Configuration();
    configuration.setString(DeploymentOptions.TARGET, "local");
    configuration.setString("celeborn.master.endpoints", "localhost:" + celebornMasterPort);
    configuration.setString("execution.batch-shuffle-mode", "ALL_EXCHANGES_BLOCKING");
    configuration.setString(
        "shuffle-service-factory.class",
        "org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory");
    return prepareEnvAndGetJobGraph(configuration, testName);
  }

  private JobGraph prepareEnvAndGetJobGraph(Configuration config, String testName)
      throws Exception {
    flinkCluster =
        TestingMiniCluster.newBuilder(getMiniClusterConfiguration(config))
            .setHighAvailabilityServicesSupplier(highAvailabilityServicesSupplier)
            .build();
    flinkCluster.start();

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(-1);
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);

    return createJobGraph(env, testName);
  }

  private TestingMiniClusterConfiguration getMiniClusterConfiguration(Configuration config)
      throws IOException {
    // flink basic configuration.
    NetUtils.Port jobManagerRpcPort = NetUtils.getAvailablePort();
    flinkConfiguration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
    flinkConfiguration.set(JobManagerOptions.PORT, jobManagerRpcPort.getPort());
    flinkConfiguration.set(JobManagerOptions.SLOT_REQUEST_TIMEOUT, Duration.ofMillis(5000L));
    flinkConfiguration.set(RestOptions.BIND_PORT, "0");
    flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
    flinkConfiguration.set(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.4F);

    // adaptive batch job scheduler config.
    flinkConfiguration.set(
        JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
    flinkConfiguration.set(
        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, DEFAULT_MAX_PARALLELISM);
    flinkConfiguration.set(
        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
        MemorySize.parse("256K"));

    // enable jm failover.
    flinkConfiguration.set(BatchExecutionOptions.JOB_RECOVERY_ENABLED, true);
    flinkConfiguration.set(BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE, Duration.ZERO);

    // region failover config.
    flinkConfiguration.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
    flinkConfiguration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    flinkConfiguration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 10);

    // ha config, which helps to trigger jm failover.
    flinkConfiguration.set(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.toString());
    highAvailabilityServices = new EmbeddedHaServicesWithLeadershipControl(executor);
    highAvailabilityServicesSupplier = () -> highAvailabilityServices;

    // shuffle dir, to help trigger partitionNotFoundException
    flinkConfiguration.set(CoreOptions.TMP_DIRS, temporaryFolder.toString());

    // add user defined config
    flinkConfiguration.addAll(config);

    return TestingMiniClusterConfiguration.newBuilder()
        .setConfiguration(flinkConfiguration)
        .setNumTaskManagers(numTaskManagers)
        .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
        .build();
  }

  public void triggerJMFailover(JobID jobId) throws Exception {
    highAvailabilityServices.revokeJobMasterLeadership(jobId).get();
    highAvailabilityServices.grantJobMasterLeadership(jobId);
  }

  public static void checkCountResults() {
    Map<Integer, Integer> countResults = StubRecordSink.countResults;
    assertThat(countResults.size()).isEqualTo(NUMBER_KEYS);

    Map<Integer, Integer> expectedResult =
        IntStream.range(0, NUMBER_KEYS)
            .boxed()
            .collect(Collectors.toMap(Function.identity(), i -> NUMBER_OF_EACH_KEY));
    assertThat(countResults).isEqualTo(expectedResult);
  }

  private JobGraph createJobGraph(StreamExecutionEnvironment env, String jobName) {
    TupleTypeInfo<Tuple2<Integer, Integer>> typeInfo =
        new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

    env.fromSequence(0, NUMBER_KEYS * NUMBER_OF_EACH_KEY - 1)
        .setParallelism(SOURCE_PARALLELISM)
        .slotSharingGroup("group1")
        .transform("SourceTail", TypeInformation.of(Long.class), new SourceTail())
        .setParallelism(SOURCE_PARALLELISM)
        .slotSharingGroup("group1")
        .transform("Map", typeInfo, new StubMapFunction())
        .slotSharingGroup("group2")
        .keyBy(tuple2 -> tuple2.f0)
        .sum(1)
        .slotSharingGroup("group3")
        .transform("Sink", TypeInformation.of(Void.class), new StubRecordSink())
        .slotSharingGroup("group4");

    StreamGraph streamGraph = env.getStreamGraph();
    streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
    streamGraph.setJobType(JobType.BATCH);
    streamGraph.setJobName(jobName);
    return StreamingJobGraphGenerator.createJobGraph(streamGraph);
  }

  private static void setSubtaskBlocked(
      List<Integer> indices, boolean block, Map<Integer, Boolean> subtaskBlocked) {
    indices.forEach(index -> subtaskBlocked.put(index, block));
  }

  /**
   * A stub which helps to:
   *
   * <p>1. Get source tasks' information. (Such as {@link ResultPartitionID}).
   *
   * <p>2. Manually control the execution of source task. Helps to block and unblock execution of
   * source task.
   *
   * <p>This operator should be chained with source operator.
   */
  public static class SourceTail extends AbstractStreamOperator<Long>
      implements OneInputStreamOperator<Long, Long> {

    public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
    public static Map<Integer, ResultPartitionID> resultPartitions = new ConcurrentHashMap<>();
    public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();

    public SourceTail() {
      super();
    }

    @Override
    public void setup(
        StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Long>> output) {
      super.setup(containingTask, config, output);

      int subIdx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

      // attempt id ++
      attemptIds.compute(
          subIdx,
          (ignored, value) -> {
            if (value == null) {
              value = 0;
            } else {
              value += 1;
            }
            return value;
          });

      if (subIdx == 0) {
        System.out.println(
            "SourceTail first task execute setup, attemptId "
                + getRuntimeContext().getTaskInfo().getAttemptNumber()
                + " , record attemptId: "
                + attemptIds.get(subIdx));
      }

      // record result partition id.
      Environment environment = getContainingTask().getEnvironment();
      checkState(environment.getAllWriters().length == 1);
      resultPartitions.put(subIdx, environment.getAllWriters()[0].getPartitionId());

      // wait until unblocked.
      if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
        tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
      }
    }

    @Override
    public void processElement(StreamRecord<Long> streamRecord) throws Exception {
      output.collect(streamRecord);
    }

    public static void clear() {
      subtaskBlocked.clear();
      attemptIds.clear();
      resultPartitions.clear();
    }

    public static void blockSubTasks(Integer... subIndices) {
      setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
    }

    public static void unblockSubTasks(Integer... subIndices) {
      setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
    }
  }

  /**
   * A special map function which can get tasks' information (Such as {@link ResultPartitionID}) and
   * manually control the task's execution.
   */
  public static class StubMapFunction extends AbstractStreamOperator<Tuple2<Integer, Integer>>
      implements OneInputStreamOperator<Long, Tuple2<Integer, Integer>> {

    public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
    public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();

    @Override
    public void setup(
        StreamTask<?, ?> containingTask,
        StreamConfig config,
        Output<StreamRecord<Tuple2<Integer, Integer>>> output) {
      super.setup(containingTask, config, output);

      int subIdx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

      // attempt id ++
      attemptIds.compute(
          subIdx,
          (ignored, value) -> {
            if (value == null) {
              value = 0;
            } else {
              value += 1;
            }
            return value;
          });

      // wait until unblocked.
      if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
        tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
      }
    }

    @Override
    public void processElement(StreamRecord<Long> streamRecord) throws Exception {
      int number = streamRecord.getValue().intValue();
      output.collect(new StreamRecord<>(new Tuple2<>(number % NUMBER_KEYS, 1)));
    }

    public static void clear() {
      subtaskBlocked.clear();
      attemptIds.clear();
    }

    public static void blockSubTasks(Integer... subIndices) {
      setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
    }

    public static void unblockSubTasks(Integer... subIndices) {
      setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
    }
  }

  /** A special sink function which can control the task's execution. */
  public static class StubRecordSink extends AbstractStreamOperator<Void>
      implements OneInputStreamOperator<Tuple2<Integer, Integer>, Void> {

    public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
    public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();
    public static Map<Integer, Integer> countResults = new ConcurrentHashMap<>();

    @Override
    public void setup(
        StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Void>> output) {
      super.setup(containingTask, config, output);

      int subIdx = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

      // attempt id ++
      attemptIds.compute(
          subIdx,
          (ignored, value) -> {
            if (value == null) {
              value = 0;
            } else {
              value += 1;
            }
            return value;
          });

      // wait until unblocked.
      if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
        tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
      }
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, Integer>> streamRecord)
        throws Exception {
      Tuple2<Integer, Integer> value = streamRecord.getValue();
      countResults.put(value.f0, value.f1);
    }

    public static void clear() {
      subtaskBlocked.clear();
      attemptIds.clear();
      countResults.clear();
    }

    public static void blockSubTasks(Integer... subIndices) {
      setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
    }

    public static void unblockSubTasks(Integer... subIndices) {
      setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
    }
  }

  public static void tryWaitUntilCondition(SupplierWithException<Boolean, Exception> condition) {
    try {
      CommonTestUtils.waitUntilCondition(condition);
    } catch (Exception exception) {
    }
  }
}
