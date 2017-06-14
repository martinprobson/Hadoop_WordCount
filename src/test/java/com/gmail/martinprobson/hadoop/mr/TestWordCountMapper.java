package com.gmail.martinprobson.hadoop.mr;


import java.io.IOException;

import org.junit.Test;

import com.gmail.martinprobson.hadoop.mr.WordCountMapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;


public class TestWordCountMapper {

	@Test
	public void processValidRecord() throws IOException, InterruptedException {
		Text value = new Text(	"The Tragedie of Macbeth\n" +
								"Who comes here?\n");
		
		MapDriver<LongWritable,Text,Text,IntWritable> mapDriver = new MapDriver<LongWritable,Text,Text,IntWritable>()
		.withMapper(new WordCountMapper())
		.withInput(new LongWritable(0),value);
		mapDriver.addOutput(new Text("The"),new IntWritable(1));
		mapDriver.addOutput(new Text("Tragedie"),new IntWritable(1));
		mapDriver.addOutput(new Text("of"),new IntWritable(1));
		mapDriver.addOutput(new Text("Macbeth"),new IntWritable(1));
		mapDriver.addOutput(new Text("Who"),new IntWritable(1));
		mapDriver.addOutput(new Text("comes"),new IntWritable(1));
		mapDriver.addOutput(new Text("here"),new IntWritable(1));
		mapDriver.runTest();
	}
}
