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

package org.apache.celeborn.plugin.flink.recover;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.recover.RecoverableStore;
import org.apache.celeborn.client.recover.operationlog.OperationLog;
import org.apache.celeborn.client.recover.operationlog.OperationLogSerializerManager;
import org.apache.celeborn.plugin.flink.recover.operationlog.AppRegisterOperationLog;

/**
 * Referring to org.apache.flink.runtime.jobmaster.event.FileSystemJobEventStore, which was
 * introduced in flink 1.20.
 *
 * <p>Implementation of {@link RecoverableStore} that stores all {@link OperationLog} instances in a
 * {@link FileSystem}. Events are written and read sequentially, ensuring a consistent order of
 * events.
 *
 * <p>Write operations to the file system are primarily asynchronous, leveraging a {@link
 * ScheduledExecutorService} to periodically flush the buffered output stream, which executes write
 * tasks at an interval determined by the flink configure option
 * "job-event.store.write-buffer.flush-interval". Note that the first operation log of the log file
 * must be AppRegisterOperationLog.
 *
 * <p>Read operations are performed synchronously, with the calling thread directly interacting with
 * the file system to fetch event data.
 */
public class FileSystemRecoverableStore implements RecoverableStore {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemRecoverableStore.class);
  private final RecoverableStoreShuffleContext storeShuffleContext;
  private final Configuration configuration;
  private final FileSystem fileSystem;
  private final Path workingDir;
  private static final String LOG_PREFIX = "operation.log.";
  private static final int INITIAL_INDEX = 0;
  private Path writeFile;
  private int writeIndex;
  private DataOutputStream outputStream;
  private int readIndex;
  private List<Path> readFiles;
  private DataInputStream inputStream;
  private ScheduledExecutorService opLogWriterExecutor;
  private final long flushIntervalInMs;
  private boolean isRecovering;
  private OperationLog idOperationLog;
  // OperationLog flush interval, which was introduced in flink 1.20
  private static final String OPERATION_LOG_FLUSH_INTERVAL_CONFIG_NAME =
      "job-event.store.write-buffer.flush-interval";
  private static long LOG_FILE_SIZE_LIMIT = 128 * 1024 * 1024;

  public FileSystemRecoverableStore(
      String jobId, Configuration configuration, RecoverableStoreShuffleContext storeShuffleContext)
      throws IOException {
    // if jobId is null, it means that the flink job is running in session mode
    this(
        new Path(
            HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration),
            jobId == null ? "celeborn-oplogs" : jobId + "/celeborn-oplogs"),
        configuration,
        storeShuffleContext);
  }

  public FileSystemRecoverableStore(
      Path workingDir,
      Configuration configuration,
      RecoverableStoreShuffleContext storeShuffleContext)
      throws IOException {
    this.storeShuffleContext = storeShuffleContext;
    this.configuration = configuration;
    this.workingDir = workingDir;
    this.fileSystem = workingDir.getFileSystem();
    boolean mkdirSuccess = fileSystem.exists(workingDir) || fileSystem.mkdirs(workingDir);
    LOG.info(
        "Create recoverable directory {} for celeborn, mkdirSuccess: {}.",
        workingDir,
        mkdirSuccess);
    this.flushIntervalInMs =
        Duration.parse(configuration.getString(OPERATION_LOG_FLUSH_INTERVAL_CONFIG_NAME, "PT1S"))
            .toMillis();
    this.start();
  }

  private void start() throws IOException {
    // get read files.
    try {
      readIndex = 0;
      readFiles = getAllOpLogPaths();
    } catch (IOException e) {
      throw new IOException("Cannot init filesystem opLog store under " + workingDir.getPath(), e);
    }
    // set write index
    writeIndex = readFiles.size();

    // create operation log write executor.
    this.opLogWriterExecutor = createOpLogWriterExecutor();
    opLogWriterExecutor.scheduleAtFixedRate(
        () -> {
          if (outputStream != null) {
            try {
              outputStream.flush();
            } catch (Exception e) {
              LOG.warn("Flush log file {} meet error, will not record log any more.", writeFile, e);
              closeOutputStream();
            }
          }
        },
        0L,
        flushIntervalInMs,
        TimeUnit.MILLISECONDS);
  }

  private List<Path> getAllOpLogPaths() throws IOException {
    List<Path> paths = new ArrayList<>();
    int index = INITIAL_INDEX;
    Path path = new Path(workingDir, LOG_PREFIX + index++);
    while (fileSystem.exists(path)) {
      paths.add(path);
      path = new Path(workingDir, LOG_PREFIX + index++);
    }
    return paths;
  }

  @Override
  public void registerAppIdOperation(OperationLog operationLog) {
    checkState(operationLog instanceof AppRegisterOperationLog);
    this.idOperationLog = operationLog;
  }

  @Override
  public void setRecoverFinished(boolean recoverFinished) {
    isRecovering = !recoverFinished;
  }

  @Override
  public synchronized void writeOperation(OperationLog operationLog) {
    if (isRecovering) {
      LOG.warn("Fail to write operation {}, because the celeborn is in recovering.", operationLog);
      return;
    }

    checkNotNull(fileSystem);
    checkNotNull(operationLog);

    opLogWriterExecutor.execute(
        () -> {
          try {
            if (outputStream == null || outputStream.size() > LOG_FILE_SIZE_LIMIT) {
              closeOutputStream();
              openNewOutputStream();
            }

            LOG.debug("Write celeborn operation log: {}", operationLog);
            OperationLogSerializerManager.serialize(operationLog, outputStream);
          } catch (Throwable throwable) {
            LOG.warn(
                "Write celeborn log {} into {} meet error, will not record log any more.",
                operationLog,
                writeFile,
                throwable);
            closeOutputStream();
          }
        });
  }

  private void openNewOutputStream() throws IOException {
    // get write file.
    writeFile = new Path(workingDir, LOG_PREFIX + writeIndex);
    outputStream =
        new DataOutputStream(fileSystem.create(writeFile, FileSystem.WriteMode.NO_OVERWRITE));
    LOG.debug("Celeborn operation log will be written to {}.", writeFile);
    if (writeIndex == 0) {
      OperationLogSerializerManager.serialize(idOperationLog, outputStream);
    }
    writeIndex++;
  }

  /** If current inputStream is null, try to get a new one. */
  private DataInputStream tryGetNewInputStream() throws IOException {
    if (inputStream == null) {
      if (readIndex < readFiles.size()) {
        Path logFile = readFiles.get(readIndex++);
        inputStream = new DataInputStream(fileSystem.open(logFile));
        LOG.info("Start reading celeborn operation log file {}", logFile.getPath());
      }
    }
    return inputStream;
  }

  @Override
  public OperationLog readOperation() throws IOException {
    try {
      OperationLog operationLog = null;
      while (operationLog == null) {
        try {
          if (inputStream == null && tryGetNewInputStream() == null) {
            return null;
          }

          operationLog = OperationLogSerializerManager.deserialize(inputStream);
        } catch (EOFException eof) {
          closeInputStream();
        }
      }
      return operationLog;
    } catch (Exception e) {
      throw new IOException("Cannot read next opLog from opLog store.", e);
    }
  }

  protected ScheduledExecutorService createOpLogWriterExecutor() {
    return Executors.newSingleThreadScheduledExecutor(
        new ExecutorThreadFactory("celeborn-operation-log-writer"));
  }

  private void closeOutputStream() {
    if (outputStream != null) {
      try {
        outputStream.close();
      } catch (IOException exception) {
        LOG.warn(
            "Close the output stream for {} meet error, will not record log any more.",
            writeFile,
            exception);
      } finally {
        outputStream = null;
      }
    }
  }

  private void closeInputStream() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
  }

  public synchronized void stop() {
    try {
      // clear workingDir
      clear();

      // close output stream
      opLogWriterExecutor.execute(
          () -> {
            closeOutputStream();
          });

      // close input stream
      closeInputStream();

      // close writer executor.
      if (opLogWriterExecutor != null) {
        opLogWriterExecutor.shutdown();
        try {
          if (!opLogWriterExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            opLogWriterExecutor.shutdownNow();
          }
        } catch (InterruptedException ignored) {
          opLogWriterExecutor.shutdownNow();
        }
        opLogWriterExecutor = null;
      }

    } catch (Exception exception) {
      LOG.warn("Fail to stop filesystem log store.", exception);
    }
  }

  @Override
  public boolean supportRecoverable() {
    return true;
  }

  public synchronized void clear() {
    // clear working dir if no jobs.
    if (!storeShuffleContext.hasJobs()) {
      opLogWriterExecutor.execute(
          () -> {
            try {
              if (!storeShuffleContext.hasJobs()) {
                LOG.info("clear celeborn operation log: {}", workingDir);
                closeOutputStream();
                fileSystem.delete(workingDir, true);
                writeIndex = 0;
              }
            } catch (Exception e) {
              LOG.warn("Fail to stop filesystem log store.", e);
            }
          });
    }
  }
}
