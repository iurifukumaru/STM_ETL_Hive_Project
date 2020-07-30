package ca.mcit.bigdata.project4

import java.sql.{Connection, DriverManager}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HDFS extends App {

  private val conf = new Configuration()
  val hadoopConfDir = System.getenv("HADOOP_CONF_DIR")
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))
  val fs: FileSystem = FileSystem.get(conf)
  val uri = fs.getUri

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.
    getConnection("jdbc:hive2://quickstart.cloudera:10000/winter2020_iuri;user=iuri;password=iuri")
  val stmt = connection.createStatement()



}


