package com.gmail.martinprobson.hadoop.mr;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.gmail.martinprobson.hadoop.mr.WordCountReducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class TestWordCountReducer {


	@Test
	public void returnMaxIntegerInValues() throws IOException, InterruptedException {

		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new WordCountReducer())
		.withInput(new Text("The"),Arrays.asList(new IntWritable(1)))
		.withInput(new Text("Tragedie"),Arrays.asList(new IntWritable(1)))
		.withInput(new Text("of"),Arrays.asList(new IntWritable(1)))
		.withInput(new Text("Macbeth"),Arrays.asList(new IntWritable(1),new IntWritable(1),new IntWritable(1)))
		.withOutput(new Text("The"),new IntWritable(1))
		.withOutput(new Text("Tragedie"),new IntWritable(1))
		.withOutput(new Text("of"),new IntWritable(1))
		.withOutput(new Text("Macbeth"),new IntWritable(3))
		.runTest();
	}
}
