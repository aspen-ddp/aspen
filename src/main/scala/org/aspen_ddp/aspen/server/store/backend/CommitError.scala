package org.aspen_ddp.aspen.server.store.backend

object CommitError extends Enumeration {
  val InvalidPointer: Value = Value("InvalidPointer")
  /** The pointer's ObjectId does not match the object stored at the pointer's location */
  val ObjectMismatch: Value = Value("ObjectMismatch")
  /** The backend failed to write the update to stable storage */
  val IOFailure: Value = Value("IOFailure")
  /** The slice is larger than the backend can hold */
  val SliceTooLarge: Value = Value("SliceTooLarge")
}
