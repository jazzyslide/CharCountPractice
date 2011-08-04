package com.lifexweb.app.hadoop.CharCountPractice.Normal;

import java.io.IOException;
import java.text.Normalizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lifexweb.app.hadoop.CharCountPractice.util.SurrogatePairUtil;

public class CharCountMapper extends Mapper<LongWritable, Text, LineWritable, IntWritable> {

	private static final IntWritable ONE = new IntWritable(1);
	private LineWritable keyLine = new LineWritable();
	private IntWritable fileCode = new IntWritable();
	private IntWritable valueCode = new IntWritable();
	
	@Override
	protected void map(LongWritable offset, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//文字列の正規化
		line = Normalizer.normalize(line, Normalizer.Form.NFC);
		//空白文字を削除
		line = line.replaceAll("\\s", "");
		int[] codePoints = SurrogatePairUtil.toCodePointArray(line);
		valueCode.set(0);
		
		for (int codePoint : codePoints) {
			valueCode.set(codePoint);
			fileCode.set(context.getTaskAttemptID().getTaskID().getId());
			keyLine.set(fileCode, offset, valueCode);
			context.write(keyLine, ONE);
		}		
	}
}
