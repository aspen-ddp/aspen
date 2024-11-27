
/* SETUP
 * 
 * This project depends on Flatbuffers which, unfortunately, must be manually installed.
 * To do so, download the flatbuffers source code, run "cmake ." from within the root
 * directory of the project, and then "make install" and "mvn install". This will
 * install the flatc utility as /usr/local/bin/flatc and create the flatbuffers jar file
 * in <flatbuffer_root>/target. Copy the jar file to the 'lib' folder of this project and
 * you should be good to go.
 *
 * Scala IDE project files can be generated with 'sbt eclipse'
 * 
 */

import scala.sys.process._

lazy val root = (project in file(".")).
  settings(

    name         := "aspen",
    version      := "0.1",
    scalaVersion := "3.3.3",
    organization := "org.aspen_ddp",
      
    scalacOptions ++= Seq("-feature", "-deprecation"), //, "-rewrite", "-source:3.0-migration"),

    resolvers += "mvnrepository" at "https://mvnrepository.com/artifact/",
    
    resolvers += "dCache Repository" at "https://download.dcache.org/nexus/content/repositories/releases",
    resolvers += "Oracle Repository" at "https://download.oracle.com/maven",
    resolvers += "sonatype-nexus-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",

    libraryDependencies ++= Seq(
      "org.scalatest"                    %% "scalatest"               % "3.2.18" % "test",
      "com.github.blemale"               %% "scaffeine"               % "5.2.1" % "compile",
      "org.rocksdb"                      %  "rocksdbjni"              % "8.11.3",
      "com.github.scopt"                 %% "scopt"                   % "4.1.0",
      "org.apache.logging.log4j"         %  "log4j-api"               % "2.22.0",
      "org.apache.logging.log4j"         %  "log4j-core"              % "2.22.0",
      "org.apache.logging.log4j"         %% "log4j-api-scala"         % "13.1.0",
      "org.slf4j"                        %  "slf4j-log4j12"           % "2.0.12",
      "com.lmax"                         %  "disruptor"               % "3.3.7",
      "org.dcache"                       %  "nfs4j-core"              % "0.24.0",
      "org.yaml"                         %  "snakeyaml"               % "1.25",
      "org.zeromq"                       %  "jeromq"                  % "0.6.0",
      "com.google.flatbuffers"           %  "flatbuffers-java"        % "1.12.0",
      "com.google.protobuf"              %  "protobuf-java"           % "3.13.0" % "protobuf",
    )
  )
  
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")

//parallelExecution in Test := false

 enablePlugins(PackPlugin)

/*
 Compile / sourceGenerators += Def.task {
  val base = (Compile / sourceManaged).value

  // Network Protocol
  val net_out_dir = (Compile / sourceManaged).value / "com" / "ibm" / "amoeba" / "common" / "network" / "protocol"

  val net_schema = file("schema") / "network_protocol.fbs"

  val net_generate = !net_out_dir.exists() || net_out_dir.listFiles().exists( f => net_schema.lastModified() > f.lastModified() )
  
  //println(s"Dir exists $net_out_dir, ${net_out_dir.exists()}")

  //net_out_dir.listFiles().foreach( f => println(s"$f : ${net_schema.lastModified() > f.lastModified()}") )

  if (net_generate) {
    println(s"Generating Network Protocol Source Files")
    val stdout:Int = s"flatc --java -o $base schema/network_protocol.fbs".!
    println(s"Result: $stdout")
  }
  
  net_out_dir.listFiles().toSeq
 }.taskValue
*/
 Compile / PB.targets := Seq(
  PB.gens.java -> (Compile / sourceManaged).value
 )
