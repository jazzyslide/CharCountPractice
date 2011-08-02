package com.lifexweb.app.hadoop.CharCountPractice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountReducer extends
		Reducer<LineWritable, IntWritable, Text, IntWritable> {
	
	private Text resultKey = new Text();
	private IntWritable resultValue = new IntWritable();
	
	@Override
	protected void reduce(LineWritable key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable c : counts) {
			count += c.get();
		}
		String tmpStr = String.valueOf(Character.toChars(key.getCodePoint().get()));
		resultKey.set(key.getFileCode() + "\t" + key.getOffset() + "\t" + tmpStr);
		resultValue.set(count);
		context.write(resultKey, resultValue);

	}

}
