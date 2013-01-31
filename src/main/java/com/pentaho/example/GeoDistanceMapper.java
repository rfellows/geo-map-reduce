/**
 * 
 */
package com.pentaho.example;

import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GeoDistanceMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
  private final static IntWritable one = new IntWritable(1);

  private static final LatLng ORLANDO = new LatLng(28.55, -81.33);
  private static final int[] bands = new int[] {50, 100, 500, 1000, 5000};

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // GeoDistance Line format:
    // Alaska,AK,Anchorage,61.2180556,-149.9002778,United States,US,USA

    String line = value.toString();
    String[] tokens = StringUtils.split(line, ",");

    IntWritable keyRange = new IntWritable(Integer.MAX_VALUE);

    try {
      double latitude = Double.parseDouble(tokens[3]);
      double longitude = Double.parseDouble(tokens[4]);
      String cityName = tokens[2];

      // determine how far away from Orlando that is
      LatLng point = new LatLng(latitude, longitude);
      double distance = LatLngTool.distance(ORLANDO, point, LengthUnit.MILE);

      // lets see where it falls
      for(int band : bands) {
        if (distance <= band) {
          keyRange = new IntWritable(band);
          break;
        }
      }

      System.out.println("{\n\tCity: " + cityName + "\n\tBand: " + keyRange.toString() + ",\n\tActual: " + distance + "\n} ");

    } catch (NumberFormatException nfe) {
      System.out.println(nfe.getMessage());
    } finally {
      context.write(keyRange, one);
    }

  }
}
