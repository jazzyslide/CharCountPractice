package com.lifexweb.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CharCountMapper extends Mapper<LongWritable, Text, LineWritable, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private LineWritable keyLine = new LineWritable();
	private Text valueText = new Text();
	
	@Override
	protected void map(LongWritable offset, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		char[] letters = line.toCharArray();
		valueText.clear();
		
		//行番号をReducer側で1からふる場合の空白行対応
//		if (letters.length == 0) {
//			keyLine.set(offset, valueText);
//			context.write(keyLine, ONE);
//		} else {
			for (Character letter : letters){
				if (!Character.isWhitespace(letter)){
					valueText.set(letter.toString());
					keyLine.set(offset, valueText);
					context.write(keyLine, ONE);
				}				
			}
//		}
	}
}
