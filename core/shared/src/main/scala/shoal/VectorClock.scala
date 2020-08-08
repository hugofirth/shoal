package shoal

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import shoal.VectorClock.Timestamp


final class VectorClock private (pClock: () => Timestamp, private val versions: Map[UUID,Timestamp], counter: AtomicLong) {
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
  def compareTo(that: VectorClock): Relationship = {
    if (versions == that.versions)
      Equal
    else if (isLessThan(versions, that.versions))
      HappensBefore
    else if (isLessThan(that.versions, versions))
      HappensAfter
    else
      HappensConcurrent
  }

  private def isLessThan(lVersions: Map[UUID,Timestamp], rVersions: Map[UUID,Timestamp]): Boolean = {
    val allLeftLTE = lVersions.forall { case (id, ts) => ts <= rVersions.getOrElse(id, timeZero) }
    val oneRightGT = rVersions.exists { case (id, ts) => ts > lVersions.getOrElse(id, timeZero) }
    allLeftLTE && oneRightGT
  }

  override def equals(other: Any): Boolean = other match {
    case that: VectorClock => compareTo(that) == Equal
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(versions)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object VectorClock {

  private val globalCounter = new AtomicLong(0)

  def apply(pClock: () => Timestamp = System.currentTimeMillis,
            versions: Map[UUID,Timestamp] = Map.empty,
            counter: AtomicLong = globalCounter): VectorClock =
    new VectorClock(pClock, versions, counter)

  /**
    * Logical timestamp, represented as a simple long
    */
  type Timestamp = Long
  final val timeZero: Timestamp = 0L

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
