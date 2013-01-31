package com.pentaho.example;

import java.util.Vector;
import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class GeoDistanceReducerTest extends TestCase {
	private Reducer<IntWritable, IntWritable, IntWritable, IntWritable> reducer;
	private ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable> driver;
	
	@Before
	public void setUp() {
		reducer = new GeoDistanceReducer();
		driver = new ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>(reducer);
	}
	
	@Test
	public void testCountCitiesInBands() {
		int expectedCityCount = 10;
		
		Vector<IntWritable> tenOnes = new Vector<IntWritable>();
		
		for (int i = 0; i < expectedCityCount; i++) {
			tenOnes.add( new IntWritable(1) );
		}
		
		driver.withInput(new IntWritable(50), tenOnes)
			  .withOutput(new IntWritable(50), new IntWritable(expectedCityCount))
			  .runTest();
	}
}
