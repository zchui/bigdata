import java.time.{LocalDate, ZoneId}
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.util.Date

import scala.collection.JavaConversions

class PeerPersonParam(val faceLibIds: Set[String], val dateEnd: LocalDate, val distance: Long, val peerInterval: Long, val before: Int, val times: Int, val beforeType: TemporalUnit) extends Serializable {
  val dateStart: LocalDate = dateEnd.plus(0 - before, beforeType)

  lazy val dateTimeEnd: Long = {
    Date.from(dateEnd.atStartOfDay(PeerPersonParam.zoneId).toInstant).getTime + PeerPersonParam.dateUnit - 1
  }

  lazy val dateTimeStart: Long = {
    Date.from(dateStart.atStartOfDay(PeerPersonParam.zoneId).toInstant).getTime
  }

}

object PeerPersonParam {
  val dateUnit: Long = 1000 * 60 * 60 * 24
  private val zoneId = ZoneId.systemDefault()

  //  def applySimple(faceLibIds: Set[String]): PeerPersonParam = {
  //    this.apply(faceLibIds)
  //  }
  //
  //  def applySimple(faceLibIds: java.util.Set[String]): PeerPersonParam = {
  //    this.apply(JavaConversions.asScalaSet(faceLibIds).toSet)
  //  }
  //
  //  def applySimple(faceLibIds: Set[String], date: LocalDate): PeerPersonParam = {
  //    this.apply(faceLibIds, date)
  //  }
  //
  //  def applySimple(faceLibIds: java.util.Set[String], date: LocalDate): PeerPersonParam = {
  //    this.apply(JavaConversions.asScalaSet(faceLibIds).toSet, date)
  //  }
  //
  //  def applySimple(faceLibIds: Set[String], date: LocalDate, distance: Long): PeerPersonParam = {
  //    this.apply(faceLibIds, date, distance)
  //  }
  //
  //  def applySimple(faceLibIds: java.util.Set[String], date: LocalDate, distance: Long): PeerPersonParam = {
  //    this.apply(JavaConversions.asScalaSet(faceLibIds).toSet, date, distance)
  //  }
  //
  //  def applySimple(faceLibIds: Set[String], date: LocalDate, distance: Long, peerInterval: Long): PeerPersonParam = {
  //    this.apply(faceLibIds, date, distance, peerInterval)
  //  }
  //
  //  def applySimple(faceLibIds: Set[String], date: LocalDate, distance: Long, peerInterval: Long, before: Int): PeerPersonParam = {
  //    this.apply(faceLibIds, date, distance, before)
  //  }
  //
  //  def applySimple(faceLibIds: java.util.Set[String], date: LocalDate, distance: Long, peerInterval: Long, before: Int): PeerPersonParam = {
  //    this.apply(JavaConversions.asScalaSet(faceLibIds).toSet, date, distance, peerInterval, before)
  //  }
  //
  //  def applySimple(faceLibIds: Set[String], date: LocalDate, distance: Long, peerInterval: Long, before: Int, beforeType: TemporalUnit): PeerPersonParam = {
  //    this.apply(faceLibIds, date, distance, peerInterval, before, beforeType)
  //  }
  def applySimple(faceLibIds: java.util.Set[String], distance: Long, peerInterval: Long, before: Int, times: Int): PeerPersonParam = {
    this.apply(JavaConversions.asScalaSet(faceLibIds).toSet, LocalDate.now(), distance, peerInterval, before, times)
  }

  //  def applySimple(faceLibIds: java.util.Set[String], date: LocalDate, distance: Long, peerInterval: Long, before: Int, beforeType: TemporalUnit): PeerPersonParam = {
  //    this.apply(JavaConversions.asScalaSet(faceLibIds).toSet, date, distance, peerInterval, before, beforeType)
  //  }

  def apply(faceLibIds: Set[String], date: LocalDate, distance: Long, peerInterval: Long, before: Int, times: Int, beforeType: TemporalUnit = ChronoUnit.DAYS): PeerPersonParam = new PeerPersonParam(faceLibIds, date, distance, peerInterval, before, times, beforeType)
}
