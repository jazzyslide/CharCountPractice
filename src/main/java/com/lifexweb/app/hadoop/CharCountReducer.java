package com.lifexweb.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountReducer extends
		Reducer<LineWritable, IntWritable, Text, IntWritable> {

	private IntWritable countResult = new IntWritable(0);
	private Text keyText = new Text();
	private long rowCount = 1;
	private long offset;
	
	@Override
	protected void reduce(LineWritable key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {

		if (offset != key.getOffset().get()) {
			rowCount++;
			offset = key.getOffset().get();
		}
		
		if (key.getText() != null && !key.getText().toString().isEmpty()) {
			int count = 0;
			for (IntWritable c : counts) {
				count += c.get();
			}
			
			keyText.set(rowCount + "\t" + key.getText().toString());
			countResult.set(count);
			context.write(keyText, countResult);
		}

	}

}
