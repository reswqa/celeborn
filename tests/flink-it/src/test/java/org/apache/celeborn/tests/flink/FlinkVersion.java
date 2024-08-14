package org.apache.celeborn.tests.flink;

import org.apache.flink.annotation.Public;

/** All supported flink versions. */
@Public
public enum FlinkVersion {
  v1_14("1.14"),
  v1_15("1.15"),
  v1_16("1.16"),
  v1_17("1.17"),
  v1_18("1.18"),
  v1_19("1.19"),
  v1_20("1.20");

  private final String versionStr;

  FlinkVersion(String versionStr) {
    this.versionStr = versionStr;
  }

  public static FlinkVersion fromVersionStr(String versionStr) {
    switch (versionStr) {
      case "1.14":
        return v1_14;
      case "1.15":
        return v1_15;
      case "1.16":
        return v1_16;
      case "1.17":
        return v1_17;
      case "1.18":
        return v1_18;
      case "1.19":
        return v1_19;
      case "1.20":
        return v1_20;
      default:
        throw new IllegalArgumentException("Unsupported flink version: " + versionStr);
    }
  }

  @Override
  public String toString() {
    return versionStr;
  }

  public boolean isNewerOrEqualVersionThan(FlinkVersion otherVersion) {
    return this.ordinal() >= otherVersion.ordinal();
  }
}
