#!/bin/bash

if [[ $1 == "bootstrap" ]]
then
  sbt compile

  rm -r local/node_a
  rm -r local/node_b
  rm -r local/node_c
fi

export CLASSPATH="/Users/tcocagne/devel/tom/aspen/target/scala-3.3.3/classes:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala3-library_3/3.3.3/scala3-library_3-3.3.3.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.24.4/protobuf-java-3.24.4.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/github/blemale/scaffeine_3/5.2.1/scaffeine_3-5.2.1.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/rocksdb/rocksdbjni/8.11.3/rocksdbjni-8.11.3.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/github/scopt/scopt_3/4.1.0/scopt_3-4.1.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.22.1/log4j-api-2.22.1.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.22.0/log4j-core-2.22.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api-scala_3/13.1.0/log4j-api-scala_3-13.1.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/lmax/disruptor/3.3.7/disruptor-3.3.7.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/download.dcache.org/nexus/content/repositories/releases/org/dcache/nfs4j-core/0.24.0/nfs4j-core-0.24.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/yaml/snakeyaml/1.25/snakeyaml-1.25.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/zeromq/jeromq/0.6.0/jeromq-0.6.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/flatbuffers/flatbuffers-java/1.12.0/flatbuffers-java-1.12.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.12/scala-library-2.13.12.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/github/ben-manes/caffeine/caffeine/3.1.1/caffeine-3.1.1.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/modules/scala-java8-compat_3/1.0.2/scala-java8-compat_3-1.0.2.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/slf4j/slf4j-reload4j/2.0.12/slf4j-reload4j-2.0.12.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.12/slf4j-api-2.0.12.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/download.dcache.org/nexus/content/repositories/releases/org/dcache/oncrpc4j-core/3.2.0/oncrpc4j-core-3.2.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/download.oracle.com/maven/com/sleepycat/je/7.3.7/je-7.3.7.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/eu/neilalexander/jnacl/1.0.0/jnacl-1.0.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/checkerframework/checker-qual/3.22.0/checker-qual-3.22.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/errorprone/error_prone_annotations/2.14.0/error_prone_annotations-2.14.0.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/ch/qos/reload4j/reload4j/1.2.22/reload4j-1.2.22.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/google/j2objc/j2objc-annotations/1.3/j2objc-annotations-1.3.jar:/Users/tcocagne/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/glassfish/grizzly/grizzly-framework/3.0.0/grizzly-framework-3.0.0.jar"

export JAVA_HOME="/opt/homebrew/Cellar/openjdk/22.0.2/libexec/openjdk.jdk/Contents/Home"

$JAVA_HOME/bin/java -cp $CLASSPATH org.aspen_ddp.aspen.demo.Main "$@"

if [[ $1 == "bootstrap" ]]
then

  mkdir -p local/node_a/stores
  mkdir -p local/node_b/stores
  mkdir -p local/node_c/stores

  mv local/bootstrap/00000000-0000-0000-0000-000000000000:0 local/node_a/stores
  mv local/bootstrap/00000000-0000-0000-0000-000000000000:1 local/node_b/stores
  mv local/bootstrap/00000000-0000-0000-0000-000000000000:2 local/node_c/stores

fi
