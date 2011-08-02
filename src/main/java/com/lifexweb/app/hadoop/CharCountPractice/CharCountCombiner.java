package com.lifexweb.app.hadoop.CharCountPractice;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountCombiner extends
		Reducer<LineWritable, IntWritable, LineWritable, IntWritable> {
	
	private IntWritable resultValue = new IntWritable();
	
	@Override
	protected void reduce(LineWritable key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable c : counts) {
			count += c.get();
		}
		resultValue.set(count);
		context.write(key, resultValue);

	}

}
