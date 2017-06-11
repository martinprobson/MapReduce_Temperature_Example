package com.gmail.martinprobson.hadoop.mr;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.gmail.martinprobson.hadoop.mr.MaxTemperatureReducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class TestMaxTemperatureReducer {


	@Test
	public void returnMaxIntegerInValues() throws IOException, InterruptedException {

		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxTemperatureReducer())
		.withInput(new Text("1960"),Arrays.asList(new IntWritable(-22),new IntWritable(1),new IntWritable(34)))
		.withOutput(new Text("1960"),new IntWritable(34))
		.runTest();
	}
}
