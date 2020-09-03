package shoal

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import shoal.VectorClock.Node
import shoal.VectorClock.Timestamp

import scala.collection.MapView
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class VectorClock private (pClock: () => Timestamp,
                                 private val nodes: Map[Node,Int],
                                 private val timestamps: ArraySeq[Timestamp],
                                 counter: AtomicLong) extends Equals {
  import VectorClock._

  /**
    * We take a hybrid logical clock approach to producing timestamps from this clock.
    * I.e. where possible we use the provided physical clock, but we always preserve the logical clock
    * guarantee of growing monotonically.
    */
  def timestamp: Timestamp = {
    var lastTimestamp = timeZero
    var nextTimestamp = timeZero

    do {
      val currentTimestamp = pClock()
      lastTimestamp = counter.get
      nextTimestamp = if ( currentTimestamp > lastTimestamp ) currentTimestamp else lastTimestamp + 1;
    } while(counter.compareAndSet(lastTimestamp, nextTimestamp))

    nextTimestamp
  }

  //TODO: Investigate the optimisations done by Akka around early bail out
  //TODO: Figure out whether MapView implements correct equality semantics
  def compareTo(that: VectorClock): Relationship = {
    if (nodes.keySet == that.nodes.keySet && nodeTimestamps == that.nodeTimestamps)
      Equal
    else if (this isLessThan that)
      HappensBefore
    else if (that isLessThan this)
      HappensAfter
    else
      HappensConcurrent
  }

  private def isLessThan(that: VectorClock): Boolean = {
    val lNodeTimestamps = this.nodeTimestamps
    val rNodeTimestamps = that.nodeTimestamps
    val allLeftLTE = lNodeTimestamps.forall { case (id, ts) => ts <= rNodeTimestamps.getOrElse(id, timeZero) }
    val oneRightGT = rNodeTimestamps.exists { case (id, ts) => ts > lNodeTimestamps.getOrElse(id, timeZero) }
    allLeftLTE && oneRightGT
  }

  private def nodeTimestamps: MapView[Node,Timestamp] = nodes.view.mapValues(v => timestamps.lift(v).getOrElse(timeZero))

  def merge(that: VectorClock): VectorClock = {
    if ( this == that ) {
      this
    } else if (nodes == that.nodes) {
      // Clocks possess same nodes and timestamps are in the same order.
      // We can do a simple max-merge of timestamp arrays
      val mergedTs = (timestamps.view zip that.timestamps.view)
        .map({ case (lTs, rTs) => lTs max rTs })
        .to(ArraySeq)

      VectorClock(pClock, nodes, mergedTs, counter)
    } else if (nodes.keySet == that.nodes.keySet) {
      // Clocks possess same nodes but timestamps are in a different order.
      // For each node, get the index of the timestamp in both arrays.
      // Look up the timestamps and keep the larger in a new timestamps array.
      // Store the new timestamp at the index specified by this.nodes
      val newTimestamps = new Array[Timestamp](timestamps.size)

      for (node <- nodes.keySet;
           thisIdx <- nodes.get(node);
           thatIdx <- that.nodes.get(node);
           thisTs <- timestamps.lift(thisIdx);
           thatTs <- that.timestamps.lift(thatIdx)) {

        newTimestamps.update(thisIdx, thisTs max thatTs)
      }

      VectorClock(pClock, nodes, ArraySeq.unsafeWrapArray(newTimestamps), counter)
    } else {
      // new timestamps array
      val diffNodes = nodes.keySet diff that.nodes.keySet
      // TODO: create nodes map of that + diff
      VectorClock(pClock, nodes, timestamps, counter)
    }
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[VectorClock]

  override def equals(other: Any): Boolean =
    (this eq other.asInstanceOf[AnyRef]) || (other match {
      case that: VectorClock if that.canEqual(this) => (that compareTo this) == Equal
      case _ => false
    })

  override def hashCode: Int = nodeTimestamps.hashCode
}

object VectorClock {

  private val globalCounter = new AtomicLong(0)

  def apply(pClock: () => Timestamp = System.currentTimeMillis,
               nodes: Map[Node,Int] = Map.empty,
               timestamps: ArraySeq[Timestamp] = ArraySeq.empty,
               counter: AtomicLong = globalCounter): VectorClock =
    new VectorClock(pClock, nodes, timestamps, counter)

  /**
    * Logical timestamp, represented as a simple long
    */
  type Timestamp = Long
  final val timeZero: Timestamp = 0L

  /**
    * Node identifier. Likely eventually contains addresses etc... Initially a UUID
    */
  type Node = UUID

  /**
    * Relationships between Vector clocks.
    * Not the same as a simple ordering, because there are 4 states rather than 3:
    *
    * HAPPENS-BEFORE
    * HAPPENS-AFTER
    * EQUAL
    * HAPPENS-CONCURRENT
    */
  sealed trait Relationship
  case object HappensBefore extends Relationship
  case object HappensAfter extends Relationship
  case object Equal extends Relationship
  case object HappensConcurrent extends Relationship
}
