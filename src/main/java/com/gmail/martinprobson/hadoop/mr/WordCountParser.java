package com.gmail.martinprobson.hadoop.mr;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

/*
 * Simple helper class to parse a line for input to Mapper.
 */
class WordCountParser {
	
	private WordCountParser() {}
	
	public static List<Text> parse(Text textLine) {
		
		List<Text> wordList = new ArrayList<>();
		String line = textLine.toString().replaceAll("[^a-zA-Z0-9 ]", " ");
		String words[] = line.trim().split("\\s+");
		for (String w : words) {
			wordList.add(new Text(w));
		}
		return wordList;
		
	}
}
