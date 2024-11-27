package org.aspen_ddp.aspen

import java.io.File

import org.scalatest.BeforeAndAfter

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class FileBasedTests extends AnyFunSuite with Matchers with BeforeAndAfter {
  var tdir:File = _
  var tdirMgr: TempDirManager = _

  before {
    tdirMgr = new TempDirManager
    tdir = tdirMgr.tdir
    preTest()
  }

  after {
    preTempDirDeletion()

    tdirMgr.delete()
  }

  def preTest(): Unit = {}
  def preTempDirDeletion(): Unit = ()
}
