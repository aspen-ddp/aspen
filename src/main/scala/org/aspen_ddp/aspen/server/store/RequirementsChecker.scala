package org.aspen_ddp.aspen.server.store

import org.aspen_ddp.aspen.common.ida.EncodedAppendDelta
import org.aspen_ddp.aspen.common.objects.{Delete, Insert, Key, KeyValueOperation, ObjectId, ObjectRefcount, ObjectRevision}
import org.aspen_ddp.aspen.common.transaction.KeyValueUpdate.FullContentLock
import org.aspen_ddp.aspen.common.transaction._
import org.aspen_ddp.aspen.common.{DataBuffer, HLCTimestamp}
import org.aspen_ddp.aspen.common.util.Varint
import scribe.Logging

import scala.language.implicitConversions

object RequirementsChecker extends Logging {

  case class ObjectErr(objectId: ObjectId, err: RequirementError) extends Exception
  case class NonObjectErr(err: RequirementError) extends Exception

  /**
    * @param maxSliceSize The store's effective slice size limit. None disables the size check. See checkSliceSize.
    * @param maxKVSliceSize The limit growing KeyValue updates must fit (IDA.maxKVSliceSize, Frontend.maxKVSliceSize);
    *                 None uses maxSliceSize. See checkKVSliceSize.
    * @param applying True for the re-check of a committed transaction, which takes no new locks. Locks exist to
    *                 serialize commit votes. The transaction's outcome is already decided, so locks held by other
    *                 transactions (and the exclusive preference window) are not enforced then: only whether this
    *                 store's state satisfies the requirements. A lock holder whose requirements conflict with this
    *                 transaction's can no longer commit, so enforcing its lock would only turn this store's part of
    *                 a committed update into a missed update. One that may still commit (it changes an object this
    *                 transaction changes) is waited for before the apply instead (lockHolders, Tx.awaitLockHolders).
    * @param nanoTime Clock for the exclusive preference window
    * @param allocatedOver Objects the transaction allocates whose load found an existing state (in memory or in the
    *                 backend) rather than a fresh one. A replayed allocation must never overwrite a live object
    *                 (transaction-requirements 9.4), so every requirement on them fails with ObjectExists
    * @param allocating The objects the transaction allocates (TransactionDescription.allocatingObjects). Their initial
    *                 content is the one KeyValue update that may be blind (see BlindWrite)
    * @return Tuple of object specific errors and a list of non-object errors
    */
  def check(transactionId: TransactionId,
            txTimestamp: HLCTimestamp,
            requirements: List[TransactionRequirement],
            objects: Map[ObjectId, ObjectState],
            objectUpdates: Map[ObjectId, DataBuffer],
            maxSliceSize: Option[Long] = None,
            maxKVSliceSize: Option[Long] = None,
            applying: Boolean = false,
            nanoTime: () => Long = () => System.nanoTime(),
            allocatedOver: Set[ObjectId] = Set(),
            allocating: Set[ObjectId] = Set()):
  (Map[ObjectId, RequirementError], List[RequirementError]) = {

    def collide(oid: ObjectId, holders: Iterable[TransactionId]): Unit =
      holders.find(_ != transactionId).foreach { other =>
        throw ObjectErr(oid, TransactionCollision(other))
      }

    /** The store's copy of TransactionBuilder.refuseBlindWrites. Unless the transaction allocates the object (its
      * initial content), a key Delete needs a KeyRevision on its key, a required revision or a content lock, and a
      * change of min, max, left or right needs a required revision. A blind Delete leaves no tombstone and
      * object-level parts carry no timestamp, so neither can be ordered against other writes, and repair cannot see
      * them: a no-op blind Delete leaves no trace for its snapshot and an object-level change that does not move the
      * revision escapes a repair proof's ExclusiveRevisionLock (repair-process 1.2, Q8; transaction-requirements 2.5).
      * Enforced here too so a client that bypasses the builder cannot issue one
      */
    def refuseBlindWrites(r: KeyValueUpdate, ops: List[KeyValueOperation]): Unit =
      val oid = r.objectPointer.id
      if r.requiredRevision.isEmpty && !allocating.contains(oid) then
        val revisionChecked = r.requirements.iterator.collect { case kr: KeyValueUpdate.KeyRevision => kr.key }.toSet
        ops.foreach {
          case _: Insert =>
          case d: Delete =>
            if r.contentLock.isEmpty && !revisionChecked.contains(d.key) then throw ObjectErr(oid, BlindWrite())
          case _ => throw ObjectErr(oid, BlindWrite())
        }

    /** Object-level locks: lockedToTransaction and a KV object's content lock. Every requirement collides with them */
    def checkObjectLocks(oid: ObjectId, os: ObjectState): Unit =
      collide(oid, os.lockedToTransaction)
      os.kvState.foreach(kvs => collide(oid, kvs.contentLocked))

    /** Locks on parts of a KV object (keys, absent keys, ranges). Requirements on the whole object collide with them */
    def checkPartLocks(oid: ObjectId, os: ObjectState): Unit =
      collide(oid, os.lockedWriteTransactions)

    /** A refcount of zero means deleted, whether or not the backend still holds the state (MapBackend keeps it): every
      * requirement on a deleted object fails as on a missing one, so a replay that only locks it cannot commit
      */
    def loaded(oid: ObjectId): ObjectState =
      objects.get(oid) match
        case None => throw ObjectErr(oid, MissingObject())
        case Some(_) if allocatedOver.contains(oid) => throw ObjectErr(oid, ObjectExists())
        case Some(os) if os.metadata.refcount.count == 0 => throw ObjectErr(oid, MissingObject())
        case Some(os) =>
          if os.reloadRequired then
            throw ObjectErr(oid, StaleObjectState())
          os

    /** The held shared lock the current requirement collides with, if any. Reported only once the requirement's own
      * checks pass (see getState), so a requirement that also fails for another reason reports that reason
      */
    var sharedCollision: Option[(ObjectId, TransactionId)] = None

    /** Objects whose only error is a collision with a held shared lock, with the holder reported */
    var sharedBlocked: Map[ObjectId, TransactionId] = Map()

    /** State for every object requirement except SharedRevisionLock. `wholeObject` requirements also collide with locks
      * on parts of a KV object, and vice versa, since part-level requirements collide with object-level locks. Held
      * shared locks block it too, but that collision is raised only after the requirement's other checks (see the
      * requirement loop), and it opens the exclusive preference window only for a transaction that fails on nothing
      * else at this store: one that would get in once the shared holders release, on its retry.
      */
    def getState(oid: ObjectId, wholeObject: Boolean = true): ObjectState =
      val os = loaded(oid)
      if !applying then
        checkObjectLocks(oid, os)
        if wholeObject then
          checkPartLocks(oid, os)
        os.sharedLocks.find(_ != transactionId).foreach { sharedTxId =>
          sharedCollision = Some(oid -> sharedTxId)
        }
      os

    /** A shared lock excludes every transaction that modifies or exclusively locks the object */
    def getSharedState(oid: ObjectId): ObjectState =
      val os = loaded(oid)
      if !applying then
        checkObjectLocks(oid, os)
        checkPartLocks(oid, os)
        if os.exclusiveWaitUntil != 0 && nanoTime() - os.exclusiveWaitUntil < 0 then
          // The waiting exclusive transaction is not tracked, so the collision names this one. It is never a
          // completed transaction, which keeps the driver from treating the collision as a stale race.
          throw ObjectErr(oid, TransactionCollision(transactionId))
      os

    var objectErrors: Map[ObjectId, RequirementError] = Map()
    var nonObjectErrors: List[RequirementError] = Nil

    for (req <- requirements) {
      sharedCollision = None
      try {
        req match {
          case r: LocalTimeRequirement => checkLocalTime(r)

          case r: DataUpdate =>
            val state = getState(r.objectPointer.id)
            checkRevision(state, r.requiredRevision, txTimestamp)
            if (!objectUpdates.contains(r.objectPointer.id))
              throw ObjectErr(r.objectPointer.id, MissingObjectUpdate())

            if r.operation == DataUpdateOperation.Append || r.operation == DataUpdateOperation.WriteRange then
              // A payload that cannot be decoded is a mismatch, not an unexpected failure
              val matches =
                try
                  EncodedAppendDelta.parse(objectUpdates(r.objectPointer.id)).matches(state.data)
                catch
                  case _: IllegalArgumentException => false
              if !matches then
                throw ObjectErr(r.objectPointer.id, AppendMismatch())

            maxSliceSize.foreach { limit =>
              val update = objectUpdates(r.objectPointer.id)
              val newSize = r.operation match
                case DataUpdateOperation.Overwrite => update.size.toLong
                // The delta matched the current slice above, so applying it yields exactly newSliceLength bytes
                case DataUpdateOperation.Append | DataUpdateOperation.WriteRange =>
                  EncodedAppendDelta.parse(update).newSliceLength.toLong
              checkSliceSize(r.objectPointer.id, state.data.size.toLong, newSize, limit)
            }

          case r: KeyValueUpdate =>
            val ops = objectUpdates.get(r.objectPointer.id).map { data =>
              KeyValueOperation.decode(data, ObjectRevision(transactionId), txTimestamp)
            }
            if ops.exists(_.exists(_.isInstanceOf[Delete])) && !r.deletesKeys then
              throw ObjectErr(r.objectPointer.id, UnflaggedDelete())
            // Stores that do not hold the object rely on the flag as the transaction's pin (DescriptionValidator)
            if r.pinned && !Pins.writesCheckedKey(r, ops.getOrElse(Nil)) then
              throw ObjectErr(r.objectPointer.id, FalsePin())
            refuseBlindWrites(r, ops.getOrElse(Nil))
            val writes = ops.map(KVWrites.of)

            // Requirements and operations that lock the whole object, rather than only the keys they name
            val wholeObject = r.requiredRevision.nonEmpty || r.contentLock.nonEmpty ||
              r.requirements.exists(_.isInstanceOf[KeyValueUpdate.KeyObjectRevision]) ||
              writes.exists(_.objectLevel)
            val state = getState(r.objectPointer.id, wholeObject)
            checkKVRequirements(state, transactionId, r.requiredRevision,
              txTimestamp, r.contentLock, r.requirements, ignoreLocks = applying)

            if (ops.isEmpty)
              throw ObjectErr(r.objectPointer.id, MissingObjectUpdate())

            // checkKVRequirements guarantees kvState is defined
            val kvs = state.kvState.get

            // Every written key is locked like a key requirement, so another transaction's lock on it collides
            if !applying then
              writes.get.keys.foreach { key =>
                val holder = kvs.content.get(key) match
                  case Some(vs) => vs.lockedToTransaction
                  case None => kvs.absentKeyLocks.get(key)
                collide(r.objectPointer.id, holder)
              }

            // The measure is the serialized KVObjectState, which is what the store persists for the object
            // (RequirementsApplyer sets data = kvs.encode()), with the growth that the commit votes held beside this
            // transaction have yet to apply
            maxKVSliceSize.orElse(maxSliceSize).foreach { limit =>
              val growth = kvs.growth(ops.get, ObjectRevision(transactionId), txTimestamp)
              val reserved = state.sliceReservations.iterator.collect { case (t, g) if t != transactionId => g }
              checkKVSliceSize(r.objectPointer.id, kvs, growth, reserved, limit)
            }

          case r: RefcountUpdate => checkRefcount(getState(r.objectPointer.id), r.requiredRefcount)

          case r: VersionBump => checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)

          case r: ExclusiveRevisionLock => checkRevision(getState(r.objectPointer.id), r.requiredRevision, txTimestamp)

          case r: SharedRevisionLock =>
            checkRevision(getSharedState(r.objectPointer.id), r.requiredRevision, txTimestamp)
        }
        sharedCollision.foreach { (oid, holder) =>
          sharedBlocked += oid -> holder
          throw ObjectErr(oid, TransactionCollision(holder))
        }
      } catch {
        case err: ObjectErr => objectErrors += (err.objectId -> err.err)
        case err: NonObjectErr => nonObjectErrors = err.err :: nonObjectErrors

        // An unexpected error means the requirement could not be evaluated. Record it as a
        // failure so the store votes to abort rather than silently treating the requirement
        // as satisfied. Object-scoped requirements are attributed to their object: apply()
        // consumes only the object errors, so a failure reported as non-object would let the
        // update be applied anyway.
        case e: Throwable =>
          req match
            case tor: TransactionObjectRequirement =>
              logger.error(s"Unexpected error checking requirement $req of transaction " +
                s"$transactionId against object ${tor.objectPointer.id}. Failing the requirement.", e)
              objectErrors += (tor.objectPointer.id -> RequirementCheckFailure())

            case _ =>
              logger.error(s"Unexpected error checking requirement $req of transaction " +
                s"$transactionId. Failing the requirement.", e)
              nonObjectErrors = RequirementCheckFailure() :: nonObjectErrors
      }
    }

    // Also over errors a requirement found before loading the state (an unflagged delete, say)
    allocatedOver.foreach { oid =>
      if objects.contains(oid) then
        objectErrors += (oid -> ObjectExists())
    }

    // The exclusive preference window: new shared lockers back off so that this transaction gets in on its retry. Only
    // for a transaction blocked by nothing but held shared locks here; one that fails on anything else (a stale
    // revision, another lock, a non-object error) could not commit after the holders release, so it would only starve
    // shared lockers (transaction-requirements 4, exclusive preference window)
    val onlySharedBlocked = sharedBlocked.nonEmpty && nonObjectErrors.isEmpty && objectErrors.forall { (oid, err) =>
      sharedBlocked.get(oid).exists(holder => err == TransactionCollision(holder))
    }
    if onlySharedBlocked then
      val until = nanoTime() + SharedRevisionLock.ExclusivePreferenceWindow.toNanos
      sharedBlocked.keysIterator.foreach(oid => objects(oid).exclusiveWaitUntil = until)

    (objectErrors, nonObjectErrors)
  }

  /** The locks held on one object, as lockHolders sees them */
  trait LockView:
    /** The holders of a single-holder slot */
    def slot(s: LockSlot): Set[TransactionId]
    /** Every holder of a lock that lets it change the object (ObjectState.lockedWriteTransactions) */
    def writers: Set[TransactionId]
    def shared: Set[TransactionId]
    def keyPresent(k: Key): Boolean

  /** The locks an object state holds. Displaced holders count (ObjectState.displacedLocks) */
  final class HeldLocks(os: ObjectState) extends LockView:
    def slot(s: LockSlot): Set[TransactionId] =
      val current = s match
        case LockSlot.Object => os.lockedToTransaction
        case LockSlot.Content => os.kvState.flatMap(_.contentLocked)
        case LockSlot.Key(k) => os.kvState.flatMap(_.content.get(k)).flatMap(_.lockedToTransaction)
        case LockSlot.AbsentKey(k) => os.kvState.flatMap(_.absentKeyLocks.get(k))
      current.toSet ++ os.displacedLocks.iterator.collect { case (`s`, t) => t }
    def writers: Set[TransactionId] = os.lockedWriteTransactions
    def shared: Set[TransactionId] = os.sharedLocks
    def keyPresent(k: Key): Boolean = os.kvState.exists(_.content.contains(k))

  /** The locks `holder`'s commit vote would take on `os` (RequirementsLocker.plannedLocks), as if it alone held any */
  final class PlannedLocks(holder: TransactionId, os: ObjectState, planned: Iterable[RequirementsLocker.PlannedLock])
    extends LockView:
    import RequirementsLocker.PlannedLock
    private val me = Set(holder)
    private val slots = planned.iterator.collect { case PlannedLock.Slot(s) => s }.toSet
    // A Key slot of an absent key takes nothing (RequirementsLocker.take)
    def slot(s: LockSlot): Set[TransactionId] = s match
      case LockSlot.Key(k) if !keyPresent(k) => Set()
      case _ => if slots.contains(s) then me else Set()
    def writers: Set[TransactionId] =
      if planned.exists(_ == PlannedLock.Range) || slots.exists(s => slot(s).nonEmpty) then me else Set()
    def shared: Set[TransactionId] = if planned.exists(_ == PlannedLock.Shared) then me else Set()
    def keyPresent(k: Key): Boolean = os.kvState.exists(_.content.contains(k))

  /** The other transactions holding locks on `only` that this transaction's requirements and KeyValue writes would
    * collide with in a non-applying check (transaction-requirements 4), whatever the state's revisions, refcounts and
    * keys. Displaced holders count (ObjectState.displacedLocks). For a committed transaction that holds no locks
    * here, these are the votes its apply may run over (Tx.awaitLockHolders). The exclusive preference window is no
    * lock and is not reported.
    *
    * @param kvWrites what the transaction's KeyValue operations change (KVWrites.from)
    */
  def lockHolders(transactionId: TransactionId,
                  requirements: List[TransactionRequirement],
                  objects: Map[ObjectId, ObjectState],
                  kvWrites: Map[ObjectId, KVWrites],
                  only: Set[ObjectId]): Set[TransactionId] =
    lockHoldersIn(transactionId, requirements, kvWrites, oid =>
      if only.contains(oid) then objects.get(oid).map(HeldLocks(_)) else None)

  /** lockHolders over the locks `locks` gives for each object, None for an object left out */
  def lockHoldersIn(transactionId: TransactionId,
                    requirements: List[TransactionRequirement],
                    kvWrites: Map[ObjectId, KVWrites],
                    locks: ObjectId => Option[LockView]): Set[TransactionId] =
    var holders = Set[TransactionId]()

    /** A key requirement or write on `k`: the present key's lock, or the absent key's */
    def keyHolders(v: LockView, k: Key): Set[TransactionId] =
      if v.keyPresent(k) then v.slot(LockSlot.Key(k)) else v.slot(LockSlot.AbsentKey(k))

    for req <- requirements do
      req match
        case r: TransactionObjectRequirement =>
          locks(r.objectPointer.id).foreach: v =>
            // Object-level locks collide with every request; held shared locks with every non-shared one
            holders ++= v.slot(LockSlot.Object) ++ v.slot(LockSlot.Content)
            r match
              case _: SharedRevisionLock =>
                holders ++= v.writers
              case kv: KeyValueUpdate =>
                holders ++= v.shared
                val writes = kvWrites.get(kv.objectPointer.id)
                val wholeObject = kv.requiredRevision.nonEmpty || kv.contentLock.nonEmpty ||
                  kv.requirements.exists(_.isInstanceOf[KeyValueUpdate.KeyObjectRevision]) ||
                  writes.exists(_.objectLevel)
                if wholeObject then
                  holders ++= v.writers
                else
                  // WithinRange collides only with whole-object requests, through the range locks
                  kv.requirements.foreach:
                    case _: KeyValueUpdate.WithinRange =>
                    case _: KeyValueUpdate.KeyObjectRevision =>
                    case kr => holders ++= keyHolders(v, kr.key)
                  writes.foreach(_.keys.foreach(k => holders ++= keyHolders(v, k)))
              case _ =>
                holders ++= v.writers ++ v.shared
        case _ =>
    holders - transactionId

  /** Rejects an update whose resulting slice exceeds the limit, unless it is no larger than the current slice.
    * Allowing non-growing updates means an object that is already oversized (e.g. created before the limit
    * applied) can always be shrunk or deleted.
    */
  def checkSliceSize(objectId: ObjectId, currentSize: Long, newSize: Long, maxSliceSize: Long): Unit =
    if newSize > maxSliceSize && newSize > currentSize then
      throw ObjectErr(objectId, SliceSizeExceeded(newSize, maxSliceSize))

  /** The KeyValue form of checkSliceSize, which also counts the growth `reserved` by the commit votes holding locks on
    * the object (ObjectState.sliceReservations), at vote and at the apply re-check alike. An update that grows the
    * object (SliceGrowth.grows) fails unless the worst state any apply order can reach fits: the current slice plus
    * every positive growth, the count varint taken at the summed count. So the votes a store holds together fit
    * together (transaction-requirements R16). `maxSliceSize` is the KeyValue limit (IDA.maxKVSliceSize), so the
    * union of committed updates a read restores fits the pool's slice limit whatever each store missed. The invariant, the current slice plus every reservation fits, holds
    * after each admitted growing update, and applying or releasing a reservation only lowers that sum, so a commit
    * voter's apply re-check never fails on size. A committed transaction that holds no reservation (a store that did
    * not vote commit for it) may fail it and is skipped and reported, which costs a repair, not margin. An update that
    * grows neither bytes nor entries never fails, so an oversized object can always be shrunk or deleted; one that
    * removes bytes but adds entries counts as growing
    */
  def checkKVSliceSize(objectId: ObjectId,
                       kvs: KVObjectState,
                       growth: SliceGrowth,
                       reserved: Iterator[SliceGrowth],
                       maxSliceSize: Long): Unit =
    if growth.grows then
      val count = kvs.content.size
      var bytes = kvs.encodedSize.toLong - Varint.getUnsignedIntEncodingLength(count) + growth.bytes.max(0)
      var entries = count.toLong + growth.entries.max(0)
      reserved.foreach { g =>
        bytes += g.bytes.max(0)
        entries += g.entries.max(0)
      }
      val worst = bytes + Varint.getUnsignedIntEncodingLength(entries.min(Int.MaxValue).toInt)
      if worst > maxSliceSize then
        throw ObjectErr(objectId, SliceSizeExceeded(worst, maxSliceSize))

  /** The slice growth each KeyValue update of the transaction makes on the objects loaded here, those that grow only
    * (SliceGrowth.grows): what its commit vote reserves with its locks (RequirementsLocker.lock). An update whose data
    * is missing or fails to decode reserves nothing; such a vote never passed its check
    */
  def sliceGrowth(transactionId: TransactionId,
                  txTimestamp: HLCTimestamp,
                  requirements: List[TransactionRequirement],
                  objects: Map[ObjectId, ObjectState],
                  objectUpdates: Map[ObjectId, DataBuffer]): Map[ObjectId, SliceGrowth] =
    requirements.iterator.collect { case r: KeyValueUpdate => r.objectPointer.id }.flatMap { oid =>
      for
        os <- objects.get(oid)
        kvs <- os.kvState
        data <- objectUpdates.get(oid)
        ops <- try Some(KeyValueOperation.decode(data, ObjectRevision(transactionId), txTimestamp))
               catch case scala.util.control.NonFatal(_) => None
        growth = kvs.growth(ops, ObjectRevision(transactionId), txTimestamp)
        if growth.grows
      yield oid -> growth
    }.toMap

  /** Holds when the store's local time `now` equals, is less than, or is greater than the requirement's timestamp */
  def checkLocalTime(tsReq: LocalTimeRequirement, now: HLCTimestamp = HLCTimestamp.now): Unit = {
    val holds = tsReq.tsRequirement match {
      case LocalTimeRequirement.Requirement.Equals => now == tsReq.timestamp
      case LocalTimeRequirement.Requirement.LessThan => now < tsReq.timestamp
      case LocalTimeRequirement.Requirement.GreaterThan => now > tsReq.timestamp
    }
    if !holds then
      throw NonObjectErr(LocalTimeError())
  }

  def checkRevision(state: ObjectState, requiredRevision: ObjectRevision, txTimestamp: HLCTimestamp): Unit = {
    if (state.metadata.revision != requiredRevision)
      throw ObjectErr(state.objectId, RevisionMismatch())

    if (state.metadata.timestamp > txTimestamp)
      throw ObjectErr(state.objectId, ObjectTimestampError())
  }

  def checkRefcount(state: ObjectState, requiredRefcount: ObjectRefcount): Unit = {
    if (state.metadata.refcount != requiredRefcount)
      throw ObjectErr(state.objectId, RefcountMismatch())
  }

  def checkKVRequirements(state: ObjectState,
                          transactionId: TransactionId,
                          requiredRevision: Option[ObjectRevision],
                          txTimestamp: HLCTimestamp,
                          contentLock: Option[FullContentLock],
                          keyRequirements: List[KeyValueUpdate.KeyRequirement],
                          ignoreLocks: Boolean = false): Unit = {

    requiredRevision.foreach { rev =>
      if (rev != state.metadata.revision)
        throw ObjectErr(state.objectId, RevisionMismatch())

      if (state.metadata.timestamp > txTimestamp)
        throw ObjectErr(state.objectId, ObjectTimestampError())
    }

    /** Other transactions' lock, if any. None when locks are ignored */
    def otherLock(lock: Option[TransactionId]): Option[TransactionId] =
      if ignoreLocks then None else lock.filter(_ != transactionId)

    def checkContentLock(kvs: KVObjectState): Unit =
      otherLock(kvs.contentLocked).foreach { lockedTransactionId =>
        throw ObjectErr(state.objectId, TransactionCollision(lockedTransactionId))
      }

    def checkLock(vs: ValueState, kvs: KVObjectState): Unit = {
      otherLock(vs.lockedToTransaction).foreach { lockedTransactionId =>
        throw ObjectErr(state.objectId, TransactionCollision(lockedTransactionId))
      }
      checkContentLock(kvs)
    }


    state.kvState match {
      case None => throw ObjectErr(state.objectId, ObjectTypeError())
      case Some(kvs) =>

        // Check for full content lock
        contentLock.foreach { cl =>
          val expected = cl.fullContents.map(kr => kr.key -> kr.revision).toSet
          val actual = kvs.content.map(t => t._1 -> t._2.revision).toSet

          if (expected != actual)
            throw ObjectErr(state.objectId, ContentMismatch())

          // An empty object has no values to check, so the content lock is checked directly too
          checkContentLock(kvs)
          kvs.content.valuesIterator.foreach(checkLock(_, kvs))
        }

        for (req <- keyRequirements) {
          req match {
            case r: KeyValueUpdate.KeyRevision => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                checkLock(vs, kvs)
                if (vs.revision != r.revision)
                  throw ObjectErr(state.objectId, RevisionMismatch())
                if (vs.timestamp > txTimestamp)
                  throw ObjectErr(state.objectId, KeyTimestampError())
            }

            case r: KeyValueUpdate.KeyObjectRevision =>
              if (state.metadata.revision != r.revision)
                throw ObjectErr(state.objectId, RevisionMismatch())

            case r: KeyValueUpdate.WithinRange =>
              kvs.min.foreach { min =>
                if (r.ordering.compare(r.key, min) < 0)
                  throw ObjectErr(state.objectId, WithinRangeError())
              }
              kvs.max.foreach { max =>
                if (r.ordering.compare(r.key, max) > 0)
                  throw ObjectErr(state.objectId, WithinRangeError())
              }

            case r: KeyValueUpdate.Exists => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) => checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.MayExist => kvs.content.get(r.key) match {
              case None => otherLock(kvs.absentKeyLocks.get(r.key)).foreach { txid =>
                throw ObjectErr(state.objectId, TransactionCollision(txid))
              }
              case Some(vs) => checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.DoesNotExist => kvs.content.get(r.key) match {
              case None => otherLock(kvs.absentKeyLocks.get(r.key)).foreach { txid =>
                throw ObjectErr(state.objectId, TransactionCollision(txid))
              }
              case Some(_) => throw ObjectErr(state.objectId, KeyExistenceError())
            }

            case r: KeyValueUpdate.TimestampEquals => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                if (vs.timestamp != r.timestamp)
                  throw ObjectErr(state.objectId, KeyTimestampError())
                checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.TimestampLessThan => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                if (!(vs.timestamp < r.timestamp))
                  throw ObjectErr(state.objectId, KeyTimestampError())
                checkLock(vs, kvs)
            }

            case r: KeyValueUpdate.TimestampGreaterThan => kvs.content.get(r.key) match {
              case None => throw ObjectErr(state.objectId, KeyExistenceError())
              case Some(vs) =>
                if (!(vs.timestamp > r.timestamp))
                  throw ObjectErr(state.objectId, KeyTimestampError())
                checkLock(vs, kvs)
            }
          }
        }
    }
  }
}
