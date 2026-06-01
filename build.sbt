
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
      "com.outr"                         %% "scribe"                  % "3.19.0",
      "com.outr"                         %% "scribe-slf4j"            % "3.19.0",
      "org.dcache"                       %  "nfs4j-core"              % "0.24.0",
      "org.yaml"                         %  "snakeyaml"               % "1.25",
      "org.zeromq"                       %  "jeromq"                  % "0.6.0",
      "com.thesamet.scalapb"             %% "scalapb-runtime"         % scalapbVersion % "protobuf",
      "com.lihaoyi"                      %% "os-lib"                  % "0.11.5",
    )
  )

  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")
  Test / testOptions += Tests.Setup(cl => cl.loadClass("org.aspen_ddp.aspen.TestLoggingConfig$").getField("MODULE$").get(null))

 Compile / PB.targets := Seq(
  scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value
 )
