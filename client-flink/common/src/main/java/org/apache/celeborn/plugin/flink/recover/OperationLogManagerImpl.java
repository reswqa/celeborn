package org.apache.celeborn.plugin.flink.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.celeborn.client.recover.OperationLogManager;
import org.apache.celeborn.client.recover.operationlog.OperationLog;

public class OperationLogManagerImpl implements OperationLogManager {

  private List<OperationLog> operationLogs = new ArrayList<>();

  @Override
  public void writeOperationLog(OperationLog operationLog) {
    operationLogs.add(operationLog);
  }

  @Override
  public OperationLog readOperationLog() throws IOException {
    return null;
  }

  @Override
  public List<OperationLog> readOperationLogs() throws IOException {
    return new ArrayList<>(operationLogs);
  }

  @Override
  public void stop() {
    operationLogs.clear();
  }

  @Override
  public void clear() {}
}
