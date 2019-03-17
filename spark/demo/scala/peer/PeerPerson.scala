import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date
import java.util.concurrent.locks.Lock

import com.amazonaws.util.json.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.abs

/**
  *
  * @param lock
  * @param sparkConf
  * @param hbaseZKHosts
  * @param hbaseZKPort
  */
class PeerPerson protected[person](val lock: Lock, val sparkConf: SparkConf, var hbaseZKHosts: String, var hbaseZKPort: Int) {
  var isClose = false
  val sc = new SparkContext(sparkConf)

  def changeHbaseAddress(hbaseZKHosts: String, hbaseZKPort: Int): Unit = {
    this.hbaseZKHosts = hbaseZKHosts
    this.hbaseZKPort = hbaseZKPort
  }

  def loadRdd(param: PeerPersonParam): RDD[(BaseData, BaseData)] = {
    val rdd = createRdd(param)
    val rddHumanInfo = createRddHumanInfo(param)
    val splitRangeRdd = doSplitRangeRdd(param, rdd, rddHumanInfo)
    val matchFilterRdd = doMatchFilterRdd(param, splitRangeRdd)
    doDisposeRdd(param, matchFilterRdd)
  }

  def sendToKafkaSimple(rdd: RDD[(BaseData, BaseData)], brokers: String, version: Int): Unit = {
    this.sendToKafka(rdd, brokers, version)
  }

  def sendToKafka(rdd: RDD[(BaseData, BaseData)], brokers: String, version: Int, partitionsSize: Int = 5): Unit = {
    val length = rdd.partitions.length
    val partRdd = if (partitionsSize > 0 && length != partitionsSize) {
      rdd.coalesce(2, length > partitionsSize)
    } else {
      rdd
    }
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    partRdd.foreachPartition((it) => {
      val producer = new KafkaProducer[String, String](props)
      try {
        it.foreach(
          (v) => {
            val jsonObj: JSONObject = new JSONObject()
            jsonObj.put("humanCredum", v._1.humanCredum)
            jsonObj.put("facetime", v._1.faceTime)
            jsonObj.put("deviceId", v._1.deviceId)
            jsonObj.put("traceUuid", v._1.traceUuid)
            jsonObj.put("faceLibId", v._1.faceLibId)
            jsonObj.put("peerHumanCredum", v._2.humanCredum)
            jsonObj.put("peerFacetime", v._2.faceTime)
            jsonObj.put("peerDeviceId", v._2.deviceId)
            jsonObj.put("peerTraceUuid", v._2.traceUuid)
            jsonObj.put("peerFaceLibId", v._2.faceLibId)
            jsonObj.put("version", version)
            producer.send(new ProducerRecord("PEER_PEOPLE", jsonObj.toString()))
          }
        )
        producer.flush()
      }
      finally {
        producer.close()
      }
    }

    )
  }

  private def createRdd(param: PeerPersonParam): RDD[(ImmutableBytesWritable, Result)] = {
    sc.newAPIHadoopRDD(hbaseConf(param), classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
  }

  private def createRddHumanInfo(param: PeerPersonParam): RDD[(ImmutableBytesWritable, Result)] = {
    sc.newAPIHadoopRDD(hbaseHumanInfoConf(param), classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
  }

  private def doSplitRangeRdd(param: PeerPersonParam, rdd: RDD[(ImmutableBytesWritable, Result)],
                              rddHumanInfo: RDD[(ImmutableBytesWritable, Result)]): RDD[(String, List[BaseData])] = {
    val rddHumain = rddHumanInfo.map(v => {
      val humanCredum = Bytes.toString(v._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("human_crednum")))
      val facelibId = Bytes.toString(v._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("facelib_id")))
      (humanCredum, facelibId)
    })
    rdd.map(v => {
      val humanCredum = Bytes.toString(v._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("human_crednum")))
      (humanCredum, v)
    }).leftOuterJoin(rddHumain) //(humanCredum,(v,facelib))
      .flatMap((v) => {
      val faceTime = Bytes.toLong(v._2._1._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("face_time")))
      val humanCredum = v._1
      val deviceId = Bytes.toString(v._2._1._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("device_id")))
      val traceUuid = Bytes.toString(v._2._1._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("trace_uuid")))
      var faceLibId = ""
      if (!v._2._2.isEmpty) {
        faceLibId = v._2._2.get
      }

      val distance = param.distance
      val secondTime = (faceTime / distance) * distance
      val thirdTime = secondTime + distance
      Iterator((s"${
        secondTime
      }_$deviceId", BaseData(Typ.Left, faceTime, humanCredum, faceLibId, deviceId, traceUuid)),
        (s"${
          thirdTime
        }_$deviceId", BaseData(Typ.Right, faceTime, humanCredum, faceLibId, deviceId, traceUuid)))
    }).combineByKey(
      List(_),
      (list: List[BaseData], value: BaseData) => {
        value :: list
      },
      (listLeft: List[BaseData], listRight: List[BaseData]) => {
        listLeft ::: listRight
      }
    )
  }

  private def doMatchFilterRdd(param: PeerPersonParam, rdd: RDD[(String, List[BaseData])]): RDD[(String, (BaseData, BaseData))] = {
    rdd.flatMap((value) => {
      //20190108再进行修改，目标是过滤重复
      val dataList = value._2
      //BaseData
      dataList.filter((data) => {
        //过滤非重点人员数据
        if (param.faceLibIds.isEmpty) {
          true
        } else {
          !data.faceLibId.equals("")
        }
      }).flatMap((data) => {
        dataList.filter((vue) => {
          var result = true
          if (vue.humanCredum == data.humanCredum) {
            result = false
          } else if (vue.typ == Typ.Right && data.typ == Typ.Right) {
            result = false
          } else if (abs(vue.faceTime - data.faceTime) > param.peerInterval) {
            result = false
          }
          result
          //          !(vue.humanId == data.humanId) && !(vue.typ == Typ.Right && data.typ == Right) && (abs(vue.faceTime - data.faceTime) <= 5000)
        }).map((vue) => {
          (data.humanCredum + vue.humanCredum, (data, vue))
          // v._1:（A的humanid+B的humanId）,v._2:(A信息,B信息)
        })
      }).toMap //toMap过滤重复的数据
        .toList
    })
  }

  private def doDisposeRdd(param: PeerPersonParam, rdd: RDD[(String, (BaseData, BaseData))]): RDD[(BaseData, BaseData)] = {
    rdd.map((v) => {
      (v._1 + v._2._1.faceTime, v._2)
      // v._1:（A的humanid+B的humanId+A的时间）,v._2:(信息A,B信息)
    }).reduceByKey((v1, v2) => {
      //去重,相同key的value进行处理，(v1，v2)表示方法,传入的参数为相同key的两个value，即为func(v1,v2)
      //此处处理的意思：对于同一条过人记录A（以时间戳“1546272000000”衡量是否同一条），与人B有多条的同行记录，则认为是一条记录。
      v2
    }).map((v) => {
      (v._2._1.humanCredum + v._2._2.humanCredum + v._2._2.faceTime, v._2)
      // v._1:（B的humanid+A的humanId+A的时间）,v._2:(信息A,B信息)
    }).reduceByKey((v1, v2) => {
      //去重,相同key的value进行处理，(v1，v2)表示方法,传入的参数为相同key的两个value，即为func(v1,v2)
      //此处处理的意思：对于多条过人记录B（以时间戳“1546272000000”衡量是否同一条），与A的同一条过人记录有多条同行记录，则认为是一条记录。
      v2
    }).map((v) => {
      //按天为单位对同行记录进行分割统计，统计每天的同行次数分别是多少
      val fm = new SimpleDateFormat("yyyy-MM-dd")
      val tim = fm.format(new Date(v._2._1.faceTime))
      (tim + v._2._1.humanCredum + v._2._2.humanCredum, v._2)
    }).combineByKey(
      (value: (BaseData, BaseData)) => {
        (List(value), 1)
      },
      (list: (List[(BaseData, BaseData)], Int), value: (BaseData, BaseData)) => {
        (value :: list._1, list._2 + 1)
      },
      (listLeft: (List[(BaseData, BaseData)], Int), listRight: (List[(BaseData, BaseData)], Int)) => {
        (listLeft._1 ::: listRight._1, listLeft._2 + listRight._2)
      }
      //humanid, (List[BaseData,BaseData],Int)
    ).filter((v) => {
      v._2._2 >= param.times
    }).flatMap((v) => {
      v._2._1
    })
  }

  private def makeScanByteArray(param: PeerPersonParam): String = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(format.format(param.dateStart) + "#"))
    scan.setStopRow(Bytes.toBytes(format.format(param.dateEnd) + ":"))
    scan.addColumn(byteInfo, byteHumanCredum)
    scan.addColumn(byteInfo, byteDeviceId)
    scan.addColumn(byteInfo, byteFaceTime)
    scan.addColumn(byteInfo, byteTraceUuid)
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterList.addFilter(new SingleColumnValueFilter(byteInfo, byteFaceTime, CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(param.dateTimeStart)))
    filterList.addFilter(new SingleColumnValueFilter(byteInfo, byteFaceTime, CompareOp.LESS_OR_EQUAL, Bytes.toBytes(param.dateTimeEnd)))
    filterList.addFilter(new SingleColumnValueFilter(byteInfo, byteHumanCredum, CompareOp.NOT_EQUAL, Bytes.toBytes("")))
    scan.setFilter(filterList)
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }

  private def makeHumanInfoScanByteArray(param: PeerPersonParam): String = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes("#"))
    scan.setStopRow(Bytes.toBytes(":"))
    scan.addColumn(byteInfo, byteHumanCredum)
    scan.addColumn(byteInfo, byteFacelibId)
    if (!param.faceLibIds.isEmpty) {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
      for (facelibId <- param.faceLibIds) {
        println(byteFacelibId)
        filterList.addFilter(new SingleColumnValueFilter(byteInfo, byteFacelibId, CompareOp.EQUAL, Bytes.toBytes(facelibId)))
      }
      scan.setFilter(filterList)
    }
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }

  private def hbaseConf(param: PeerPersonParam): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseZKHosts)
    conf.set("hbase.zookeeper.property.clientPort", String.valueOf(hbaseZKPort))
    conf.set(TableInputFormat.INPUT_TABLE, tableSnapImageInfo)
    conf.set(TableInputFormat.SCAN, makeScanByteArray(param))
    conf
  }

  private def hbaseHumanInfoConf(param: PeerPersonParam): Configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseZKHosts)
    conf.set("hbase.zookeeper.property.clientPort", String.valueOf(hbaseZKPort))
    conf.set(TableInputFormat.INPUT_TABLE, tableHumanInfo)
    conf.set(TableInputFormat.SCAN, makeHumanInfoScanByteArray(param))
    conf
  }

  def close(): Unit = {
    if (!isClose) {
      isClose = true
      try {
        if (!sc.isStopped) {
          sc.stop()
        }
      } finally {
        lock.unlock()
      }
    }
  }

}

private object PeerPerson {
  val format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMM")
  val tableSnapImageInfo = "SNAP_IMAGE_INFO"
  val tableHumanInfo = "HUMAN_INFO"
  val byteInfo: Array[Byte] = Bytes.toBytes("info")
  val byteHumanCredum: Array[Byte] = Bytes.toBytes("human_crednum")
  val byteDeviceId: Array[Byte] = Bytes.toBytes("device_id")
  val byteFaceTime: Array[Byte] = Bytes.toBytes("face_time")
  val byteTraceUuid: Array[Byte] = Bytes.toBytes("trace_uuid")
  val byteFacelibId: Array[Byte] = Bytes.toBytes("facelib_id")

}

case class BaseData(typ: Typ.Value, faceTime: Long, humanCredum: String, faceLibId: String, deviceId: String, traceUuid: String)

object Typ extends Enumeration {
  val Left = Value(0)
  val Right = Value(1)
}
