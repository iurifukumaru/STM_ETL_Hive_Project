package ca.mcit.bigdata.project4

import org.apache.hadoop.fs.Path

object HiveClient extends HDFS {

  if(fs.delete(
    new Path(
      s"$uri/user/winter2020/iuri/project4"),
      true)) println("Folder 'project4' deleted before Instantiate!")

  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/project4/trips"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/project4/calendar_dates"))
  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/project4/frequencies"))

  fs.copyFromLocalFile(new Path("file:///home/iuri/Downloads/trips.txt"),
    new Path(s"$uri/user/winter2020/iuri/project4/trips/trips.txt"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Downloads/calendar_dates.txt"),
    new Path(s"$uri/user/winter2020/iuri/project4/calendar_dates/calendar_dates.txt"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Downloads/frequencies.txt"),
    new Path(s"$uri/user/winter2020/iuri/project4/frequencies/frequencies.txt"))

  stmt.executeUpdate("""CREATE DATABASE IF NOT EXISTS winter2020_iuri""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_trips""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_calendar_dates""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_frequencies""".stripMargin)
  stmt.executeUpdate("""DROP TABLE enrichedtrip""".stripMargin)

  stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict")

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_trips (
       |route_id               INT,
       |service_id             STRING,
       |trip_id                STRING,
       |trip_headsign          STRING,
       |direction_id           INT,
       |shape_id               INT,
       |wheelchair_accessible  INT,
       |note_fr                STRING,
       |note_en                STRING
       |)
       |row format DELIMITED
       |fields TERMINATED BY ','
       |stored as textfile
       |LOCATION '/user/winter2020/iuri/project4/trips'
       |tblproperties ("skip.header.line.count"="1")""".stripMargin
   )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_frequencies (
      |trip_id       STRING,
      |start_time    STRING,
      |end_time      STRING,
      |headway_secs  INT
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/project4/frequencies'
      |tblproperties ("skip.header.line.count"="1")""".stripMargin
  )

  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE ext_calendar_dates (
      |service_id      STRING,
      |date            STRING,
      |exception_type  INT
      |)
      |row format DELIMITED
      |fields TERMINATED BY ','
      |stored as textfile
      |LOCATION '/user/winter2020/iuri/project4/calendar_dates'
      |tblproperties ("skip.header.line.count"="1")""".stripMargin
  )

   stmt.executeUpdate(
    """CREATE TABLE winter2020_iuri.enrichedtrip (
      |route_id        INT,
      |service_id      STRING,
      |trip_id         STRING,
      |trip_headsign   STRING,
      |direction_id    INT,
      |shape_id        INT,
      |note_fr         STRING,
      |note_en         STRING,
      |start_time      STRING,
      |end_time        STRING,
      |headway_secs    INT,
      |date            STRING,
      |exception_type  INT
      |)
      |PARTITIONED BY (wheelchair_accessible INT)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET""".stripMargin
  )

  stmt.executeUpdate(
    """INSERT OVERWRITE TABLE winter2020_iuri.enrichedtrip PARTITION(wheelchair_accessible)
      |SELECT route_id, t.service_id, t.trip_id, trip_headsign, direction_id, shape_id,
      |note_fr, note_en, start_time, end_time, headway_secs,
      |date, exception_type, wheelchair_accessible
      |FROM ext_trips t
      |LEFT JOIN ext_frequencies f ON (t.trip_id = f.trip_id)
      |LEFT JOIN ext_calendar_dates c ON (t.service_id = c.service_id)
      """.stripMargin
  )

  stmt.close()
  connection.close()

}
