package com.lifexweb.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountReducer extends
		Reducer<LineWritable, IntWritable, LineWritable, IntWritable> {

//	private Text keyText = new Text();
//	private long rowCount = 1;
//	private long offset;
	
	@Override
	protected void reduce(LineWritable key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (IntWritable c : counts) {
			count += c.get();
		}
		context.write(key, new IntWritable(count));
		
		
		//行番号を1から順番にふる場合の実装（Reducerは1つじゃないとダメ）
//		if (offset != key.getOffset().get()) {
//			rowCount++;
//			offset = key.getOffset().get();
//		}
//		
//		if (key.getText() != null && !key.getText().toString().isEmpty()) {
//			int count = 0;
//			for (IntWritable c : counts) {
//				count += c.get();
//			}
//			
//			keyText.set(rowCount + "\t" + key.getText().toString());
//			context.write(keyText, new IntWritable(count));
//		}

	}

}
