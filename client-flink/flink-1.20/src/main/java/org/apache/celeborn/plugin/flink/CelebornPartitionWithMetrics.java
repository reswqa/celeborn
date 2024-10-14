package org.apache.celeborn.plugin.flink;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;

import java.util.Arrays;

import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.shuffle.DefaultShuffleMetrics;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMetrics;

public class CelebornPartitionWithMetrics implements PartitionWithMetrics {
  // since celeborn cannot obtain all shuffle metrics, such as bytes of per subpartition
  // we will return a fake ShuffleMetrics, it will just impact the AdaptiveBatchScheduler infer
  // parallelism of job
  private ShuffleMetrics shuffleMetrics;
  private ShuffleDescriptor shuffleDescriptor;

  public CelebornPartitionWithMetrics(ShuffleDescriptor shuffleDescriptor) {
    this.shuffleDescriptor = shuffleDescriptor;
    checkState(
        shuffleDescriptor instanceof RemoteShuffleDescriptor,
        "Expect RemoteShuffleDescirptor, but found " + shuffleDescriptor);
    int numberOfSubpartitions =
        ((RemoteShuffleDescriptor) shuffleDescriptor).getNumberOfSubpartitions();
    long[] subpartitionBytes = new long[numberOfSubpartitions];
    // to avoid accumulate overflow in flink scheduler, not fill Long.MAX_VALUE directly
    Arrays.fill(subpartitionBytes, Long.MAX_VALUE / 10_0000);
    this.shuffleMetrics = new DefaultShuffleMetrics(new ResultPartitionBytes(subpartitionBytes));
  }

  @Override
  public ShuffleMetrics getPartitionMetrics() {
    return shuffleMetrics;
  }

  @Override
  public ShuffleDescriptor getPartition() {
    return shuffleDescriptor;
  }
}
