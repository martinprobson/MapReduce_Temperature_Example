/**
 * 
 */
package net.martinprobson.hadoop.mr;

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

	enum ParserCount {
		PARSERS
	}
	
	private MaxTemperatureParser parser;
	private static final Log log = LogFactory.getLog(MaxTemperatureMapper.class);
	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			// Increment a custom counter every time we
			// get a new parser.
			context.getCounter(ParserCount.PARSERS).increment(1);
			parser = MaxTemperatureParser.parse(value);
		} catch (ParseException e) {
			log.error(e + " - " + value);
		}
		if (parser.isValid())
			context.write(parser.getYear(), new IntWritable(parser.getTemperature()));
	}
}	
