
import scalapb.compiler.Version.scalapbVersion

lazy val root = (project in file(".")).
  settings(

    name         := "aspen",
    version      := "0.1",
    scalaVersion := "3.7.3",
    organization := "org.aspen_ddp",
      
    scalacOptions ++= Seq("-feature", "-deprecation"), //, "-rewrite", "-source", "3.7-migration"),

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
      "org.dcache"                       %  "nfs4j-core"              % "0.24.0",
      "org.yaml"                         %  "snakeyaml"               % "1.25",
      "org.zeromq"                       %  "jeromq"                  % "0.6.0",
      "com.thesamet.scalapb"             %% "scalapb-runtime"         % scalapbVersion % "protobuf",
      "com.lihaoyi"                      %% "os-lib"                  % "0.11.5",
    )
  )
  
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")

 Compile / PB.targets := Seq(
  scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value
 )
