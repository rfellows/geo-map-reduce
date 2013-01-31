package com.pentaho.example;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GeoDistance extends Configured implements Tool {
	
  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());
    job.setJarByClass(GeoDistance.class);
    job.setJobName("geo-map-reduce");

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(GeoDistanceMapper.class);
    job.setCombinerClass(GeoDistanceReducer.class);
    job.setReducerClass(GeoDistanceReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    for(int i = 0; i < args.length; i++) {
      System.out.println("agrs[" + i + "] " + args[i]);
    }

    FileInputFormat.setInputPaths(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new GeoDistance(), args);
    System.exit(ret);
  }

}
