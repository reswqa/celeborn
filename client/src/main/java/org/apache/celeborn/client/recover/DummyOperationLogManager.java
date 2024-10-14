package org.apache.celeborn.client.recover;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.celeborn.client.recover.operationlog.OperationLog;

public class DummyOperationLogManager implements OperationLogManager {
  @Override
  public void writeOperationLog(OperationLog operationLog) {}

  @Override
  public OperationLog readOperationLog() throws IOException {
    return null;
  }

  @Override
  public List<OperationLog> readOperationLogs() {
    return Collections.emptyList();
  }

  @Override
  public void stop() {}

  @Override
  public void clear() {}

  @Override
  public boolean supportRecoverable() {
    return false;
  }
}
