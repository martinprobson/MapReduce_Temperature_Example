/**
 * 
 */
package com.gmail.martinprobson.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example Java Mapper class (using the 'new' API)
 * Taken from Hadoop - The Definitive Guide - 4th Edition - Chapter 2
 *
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Log log = LogFactory.getLog(MaxTemperatureMapper.class);
		String line = value.toString();
		log.info("Line is:" + line);
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
}	
