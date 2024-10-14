package org.apache.celeborn.plugin.flink;

import java.util.List;

import org.apache.flink.runtime.shuffle.ShuffleMasterSnapshot;

import org.apache.celeborn.client.recover.operationlog.OperationLog;

class CelebornShuffleMasterSnapshot implements ShuffleMasterSnapshot {
  private static final long serialVersionUID = 883412528056770752L;

  private List<OperationLog> operationLogs;

  public CelebornShuffleMasterSnapshot(List<OperationLog> operationLogs) {
    this.operationLogs = operationLogs;
  }

  public List<OperationLog> getOperationLogs() {
    return operationLogs;
  }

  @Override
  public boolean isIncremental() {
    return true;
  }
}
