geo-map-reduce
==============

Add the files in the data folder to HDFS.

Run the maven package target
  mvn package

from your geo-map-reduce folder, execute this to run the job:

  hadoop jar target/geo-map-reduce-job.jar com.pentaho.example.GeoDistance <<hdfs folder where you put the input files>> <<some hdfs output folder>>

