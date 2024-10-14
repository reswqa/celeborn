package org.apache.celeborn.client.recover;

import java.io.IOException;
import java.util.List;

import org.apache.celeborn.client.recover.operationlog.OperationLog;

/**
 * Used to record RemoteShuffleMaster/CelebornTierMasterAgent/LifecycleManager actions into
 * persistent log, and used to recover LifecycleManager upon flink JobManager recovery.
 */
public interface OperationLogManager {
  /**
   * Used to reduce the number persistent logs.
   *
   * <p>For example, CommitHandler#commitEpoch, which represents the commit times of the
   * LifecycleManager, should be unique in a job and does not need to be continuous, even if the job
   * has failed and is being recovered. In this case, we only record the maximum number of
   * CommitHandler#commitEpoch (which may not have been reached), and after job recovery,
   * CommitHandler#commitEpoch will be set to the recorded epoch number. e.g. When epoch = 0, record
   * epoch = 10000 in the persistent log, and after recovery, CommitHandler#commitEpoch will be set
   * to 10000; When epoch = 1,2,3...9999, do not record to persistent log, and after recovery,
   * CommitHandler#commitEpoch will be set to 10000; When epoch = 10000, record epoch = 20000 in the
   * persistent log, and after recovery, CommitHandler#commitEpoch will be set to 20000;
   */
  int ID_PERSISTENT_STEP = 10000;

  /** Write operation log into persistent. */
  void writeOperationLog(OperationLog operationLog);

  /** Read operationLog from internal storage. */
  OperationLog readOperationLog() throws IOException;

  List<OperationLog> readOperationLogs() throws IOException;

  void stop();

  void clear();

  /** Whether the job can recover from this RecoverableStore. */
  default boolean supportRecoverable() {
    return true;
  }
}
