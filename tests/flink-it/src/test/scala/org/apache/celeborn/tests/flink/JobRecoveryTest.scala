package org.apache.celeborn.tests.flink

import JobRecoveryHelper.{SOURCE_PARALLELISM, SourceTail, StubMapFunction, StubRecordSink}
import org.assertj.core.api.Assertions.assertThat
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

/** Job Recovery Test, referring to org.apache.flink.test.scheduling.JMFailoverITCase. */
class JobRecoveryTest extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll with BeforeAndAfterEach {
  var workers: collection.Set[Worker] = null
  var celebornMasterPort = 0
  val jobRecoveryHelper = new JobRecoveryHelper

  protected def getMasterConf: Map[String, String] = Map()
  protected def getWorkerConf: Map[String, String] = Map()
  protected def getWorkerNum: Int = 3
  protected def getClientConf: Map[String, String] = Map()

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val (m, w) = setupMiniClusterWithRandomPorts(getMasterConf, getWorkerConf, getWorkerNum)
    workers = w
    celebornMasterPort = m.conf.get(CelebornConf.MASTER_PORT)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  override def beforeEach(): Unit = {
    jobRecoveryHelper.before()
  }

  override def afterEach(): Unit = {
    jobRecoveryHelper.after()
  }

  private def assumeFlinkVersion(): Unit = {
    // Job recovery from JobManager failover was introduced in 1.20.
    val flinkVersion = sys.env.getOrElse("FLINK_VERSION", "")
    assume(
      flinkVersion.nonEmpty && flinkVersion.startsWith("1.20"))
  }

  test("flink job with celeborn recovery test") {
//    assumeFlinkVersion()
    val jobGraph =
      jobRecoveryHelper.prepareEnvAndGetJobGraph(celebornMasterPort, "celeborn_rss_recovery_test")

    // blocking all sink
    StubRecordSink.blockSubTasks(0, 1, 2, 3)

    val jobId = jobRecoveryHelper.flinkCluster.submitJob(jobGraph).get.getJobID

    // wait until sink is running.
    JobRecoveryHelper.tryWaitUntilCondition(() => StubRecordSink.attemptIds.size > 0)

    jobRecoveryHelper.triggerJMFailover(jobId)

    // unblock all sink.
    StubRecordSink.unblockSubTasks(0, 1, 2, 3)

    val jobResult = jobRecoveryHelper.flinkCluster.requestJobResult(jobId).get
    assertThat(jobResult.getSerializedThrowable).isEmpty

    JobRecoveryHelper.checkCountResults()

    // check already completed tasks before JM failed execute only once
    assertThat(SourceTail.attemptIds.size()).isEqualTo(SOURCE_PARALLELISM)
    SourceTail.attemptIds.values().stream().forEach((t: Integer) => assertThat(t).isEqualTo(0))
    StubMapFunction.attemptIds.values().stream().forEach((t: Integer) => assertThat(t).isEqualTo(0))

    // check unfinished task in JM failed execute again after JM recover
    StubRecordSink.attemptIds.values().stream().forEach((t: Integer) => assertThat(t).isEqualTo(1))
  }
}
