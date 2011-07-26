package com.lifexweb.app.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class CharCountMapperTest {

	private CharCountMapper mapper;
	private MapDriver<LongWritable, Text, LineWritable, IntWritable> mapDriver;
	
	@Before
	public void setUp() throws Exception {
		//テスト用Mapper作成
		mapper = new CharCountMapper();
		
		//テスト用のMapDriver作成
		mapDriver = new MapDriver<LongWritable, Text, LineWritable, IntWritable>(mapper);
	}

	@Test
	public final void testMap() {
		mapDriver.withInput(new LongWritable(0), new Text("This is a pen.\n"))
		.withOutput(new LineWritable(0, "T"), new IntWritable(1))
		.withOutput(new LineWritable(0, "h"), new IntWritable(1))
		.withOutput(new LineWritable(0, "i"), new IntWritable(1))
		.withOutput(new LineWritable(0, "s"), new IntWritable(1))
		.withOutput(new LineWritable(0, "i"), new IntWritable(1))
		.withOutput(new LineWritable(0, "s"), new IntWritable(1))
		.withOutput(new LineWritable(0, "a"), new IntWritable(1))
		.withOutput(new LineWritable(0, "p"), new IntWritable(1))
		.withOutput(new LineWritable(0, "e"), new IntWritable(1))
		.withOutput(new LineWritable(0, "n"), new IntWritable(1))
		.withOutput(new LineWritable(0, "."), new IntWritable(1))
		.runTest();
		
		mapDriver.resetOutput();
		
		mapDriver.withInput(new LongWritable(15), new Text("Test"))
		.withOutput(new LineWritable(15, "T"), new IntWritable(1))
		.withOutput(new LineWritable(15, "e"), new IntWritable(1))
		.withOutput(new LineWritable(15, "s"), new IntWritable(1))
		.withOutput(new LineWritable(15, "t"), new IntWritable(1))
		.runTest();
	}

}
