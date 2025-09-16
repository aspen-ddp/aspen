package org.aspen_ddp.aspen.common

import java.nio.ByteBuffer

import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


object DataBufferSuite:
  def db2s(db: DataBuffer): String = "[" + db.getByteArray.mkString(",") + "]"
  
  def pdb(db: DataBuffer): Unit = println(db2s(db))
  
  def bdb(barr: Int*): DataBuffer = DataBuffer(barr.map(_.toByte).toArray)
    
  
class DataBufferSuite extends AnyFunSuite with Matchers:
  import DataBufferSuite.*

  test("compareTo"):
    val arr = Array[Byte](1, 2, 3)
    val bb = ByteBuffer.wrap(arr)

    val da = DataBuffer(arr)
    val db = DataBuffer(bb)

    da.compareTo(db) should be(0)
    db.compareTo(da) should be(0)

  test("slice"):
    val arr = Array[Byte](1, 2, 3, 4, 5)
    val db = DataBuffer(arr)
    
    db.slice(0, db.size).compareTo(db) should be(0)
    db.slice(0, 2).compareTo(bdb(1, 2)) should be(0)
    db.slice(0, 2).compareTo(bdb(1, 2)) should be(0)
    db.slice(1, 2).compareTo(bdb(2, 3)) should be(0)
    db.slice(3, 2).compareTo(bdb(4, 5)) should be(0)
    db.slice(4, 1).compareTo(bdb(5)) should be(0)

  test("split"):
    val db = bdb(1, 2, 3, 4, 5)
    
    def cmp(a: (DataBuffer, DataBuffer), b: (DataBuffer, DataBuffer)): Unit =
      a._1.compareTo(b._1) should be (0)
      a._2.compareTo(b._2) should be (0)
    
    cmp(db.split(0), (DataBuffer.Empty, db))
    cmp(db.split(1), (bdb(1), bdb(2, 3, 4,5)))
    cmp(db.split(2), (bdb(1, 2), bdb(3, 4, 5)))
    cmp(db.split(3), (bdb(1, 2, 3), bdb(4, 5)))
    cmp(db.split(4), (bdb(1, 2, 3, 4), bdb(5)))
    cmp(db.split(5), (bdb(1, 2, 3, 4, 5), DataBuffer.Empty))
    

