import java.util.concurrent.locks.{Lock, ReentrantLock}

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkConf

class PeerPersonBuilder(var rmHost: String, var hdfsHost: String, clusterHosts: String, var jars: String, val appName: String = "peer-person") {
  val lock: Lock = new ReentrantLock()
  val master: String = "yarn"

  var rmAddress: String = s"$rmHost:8032"
  var rmSchedulerAddress: String = s"$rmHost:8030"
  var fsAddress: String = s"hdfs://$hdfsHost:8020"
  var hbaseZKHosts: String = s"$clusterHosts"
  var hbaseZKPort: Int = 2181
  var stagingDir: String = s"hdfs://$hdfsHost:8020/user/temp"
  var serializerClass = "org.apache.spark.serializer.KryoSerializer"
  var enableUI: Boolean = false


  lazy val sparkConf: SparkConf = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    sparkConf.set("spark.deploy.mode", "client")
    sparkConf.set("spark.ui.enabled", String.valueOf(enableUI))
    sparkConf.set(s"spark.hadoop.${YarnConfiguration.RM_ADDRESS}", rmAddress)
    sparkConf.set(s"spark.hadoop.${YarnConfiguration.RM_SCHEDULER_ADDRESS}", rmSchedulerAddress)
    sparkConf.set("spark.serializer", serializerClass)
    sparkConf.set("spark.hadoop.fs.defaultFS", fsAddress)
    sparkConf.set("spark.yarn.stagingDir", stagingDir)
    sparkConf.set("spark.yarn.jars", jars)
//    sparkConf.set("spark.executor.memory", "30g")
//    sparkConf.set("spark.executor.cores", "10")
//    sparkConf.set("spark.yarn.executor.memoryOverhead", "1024M")
//    sparkConf.set("spark.yarn.am.memory", "512M")
    sparkConf
  }


  def build(): Option[PeerPerson] = {
    val isLock = lock.tryLock()
    if (isLock) {
      Some(new PeerPerson(lock, sparkConf, hbaseZKHosts, hbaseZKPort))
    } else {
      None
    }
  }
}

object PeerPersonBuilder {
  def applySimple(rmHost: String, hdfsHost: String, clusterHosts: String, jars: String): PeerPersonBuilder = new PeerPersonBuilder(rmHost, hdfsHost, clusterHosts, jars)

  def apply(rmHost: String, hdfsHost: String, clusterHosts: String, jars: String, appName: String = "peer-person"): PeerPersonBuilder = new PeerPersonBuilder(rmHost, hdfsHost, clusterHosts, jars, appName)
}
