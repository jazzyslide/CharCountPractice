package com.lifexweb.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private Text keyText = new Text();
//	private Counter mapInputCounter;
	private int rowCount = 0;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		mapInputCounter = context.getCounter(org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS);
		rowCount++;
		String line = value.toString();
		char[] letters = line.toCharArray();
		
		for (char letter : letters){
			if (!Character.isWhitespace(letter)){
				keyText.set(rowCount + "\t" + letter);
				context.write(keyText, ONE);
			}
		}	
	}

	
	
}
