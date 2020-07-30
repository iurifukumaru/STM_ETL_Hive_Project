name := "course4project"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.7" /*in the video was 2.6.0*/

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

// Make sure you pick the version that is compatible with your cluster
// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"

// Cloudera artifacts are published in their own remote repository
resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"
