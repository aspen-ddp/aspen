package org.aspen_ddp.aspen.vdisk.tcmu

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CfgstringSuite extends AnyFunSuite with Matchers:

  test("aspen/<disk> gives the disk name"):
    Cfgstring.parse("aspen/vd1") should be (Right("vd1"))

  test("anything from the first semicolon is dropped"):
    Cfgstring.parse("aspen/vd1;foo") should be (Right("vd1"))
    Cfgstring.parse("aspen/vd1;a;b") should be (Right("vd1"))

  test("another handler prefix is rejected"):
    Cfgstring.parse("file/x") should be (Left("cfgstring must start with aspen/"))
    Cfgstring.parse("") should be (Left("cfgstring must start with aspen/"))

  test("an empty disk name is rejected"):
    Cfgstring.parse("aspen/").isLeft should be (true)
    Cfgstring.parse("aspen/;foo").isLeft should be (true)

  test("the removed @socket form is rejected"):
    val r = Cfgstring.parse("aspen/vd1@/run/x")
    r.isLeft should be (true)
    r.left.toOption.get should include ("removed")
    r.left.toOption.get should include ("aspen/<disk>")

  test("an @ after the semicolon is not part of the disk name"):
    Cfgstring.parse("aspen/vd1;x@y") should be (Right("vd1"))

  test("ConfigDesc describes what a targetcli user types"):
    Cfgstring.ConfigDesc should startWith ("<disk>: ")
    Cfgstring.ConfigDesc should include ("targetcli adds the aspen/ prefix")
