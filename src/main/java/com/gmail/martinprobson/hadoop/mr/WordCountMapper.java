/**
 * 
 */
package com.gmail.martinprobson.hadoop.mr;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example Java Mapper class (using the 'new' API)
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@SuppressWarnings("unused")
	private static final Log log = LogFactory.getLog(WordCountMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		List<Text> words =  WordCountParser.parse(value);
		for (Text word: words) 
			context.write(word,new IntWritable(1));
	}
}	
