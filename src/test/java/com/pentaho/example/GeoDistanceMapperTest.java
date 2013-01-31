package com.pentaho.example;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

public class GeoDistanceMapperTest extends TestCase {
	private static final IntWritable ONE_OCCURANCE = new IntWritable(1);
  private static final IntWritable ZERO_OCCURANCE = new IntWritable(0);

  private Mapper<LongWritable, Text, IntWritable, IntWritable> mapper;
  private MapDriver<LongWritable, Text, IntWritable, IntWritable> driver;

  @Before
  public void setUp() {
    mapper = new GeoDistanceMapper();
    driver = new MapDriver<LongWritable, Text, IntWritable, IntWritable>(mapper);
  }

  @Test
  public void testMapDistance() {

    String city = "Alaska,AK,Anchorage,61.2180556,-149.9002778,United States,US,USA";

    driver.withInput(UNUSED_LONG_KEY, new Text(city))
      .withOutput(new IntWritable(5000), ONE_OCCURANCE)
      .runTest();

    driver.resetOutput();
    city = "Florida,FL,Miami,25.7742658,-80.1936589,United States,US,USA";
    driver.withInput(UNUSED_LONG_KEY, new Text(city) )
        .withOutput(new IntWritable(500), ONE_OCCURANCE )
        .runTest();

    driver.resetOutput();
    city = "Florida,FL,Kissimmee,28.2919557,-81.407571,United States,US,USA";
    driver.withInput(UNUSED_LONG_KEY, new Text(city) )
        .withOutput(new IntWritable(50), ONE_OCCURANCE )
        .runTest();

    driver.resetOutput();
    city = "Ohio,OH,Youngstown,41.0997803,-80.6495194,United States,US,USA";
    driver.withInput(UNUSED_LONG_KEY, new Text(city) )
        .withOutput(new IntWritable(1000), ONE_OCCURANCE )
        .runTest();
  }

  private static final LongWritable UNUSED_LONG_KEY = new LongWritable(1);
}
