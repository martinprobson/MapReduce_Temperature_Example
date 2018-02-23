package net.martinprobson.hadoop.mr;


import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;


public class TestMaxTemperatureMapper {

	@Test
	public void processValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0081028640999991960010418004+65783+024583FM-12+001599999V0202101N00101000601CN0045001N" +
	//                                   Year  ^^^^
				 "9-00221-00221102411ADDAA199000691AY121999GF108991081061000751999999KA1999M-00221MD1810171+9999MW1021");
		//    Temp ^^^^^
		new MapDriver<LongWritable,Text,Text,IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(0),value)
		.withOutput(new Text("1960"),new IntWritable(-22))
		.runTest();
	}
}
