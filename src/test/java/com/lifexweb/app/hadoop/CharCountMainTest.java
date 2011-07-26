package com.lifexweb.app.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CharCountMainTest {

	private CharCountMapper mapper;
	private CharCountReducer reducer;
	private MapReduceDriver<LongWritable, Text, LineWritable, IntWritable, Text, IntWritable > driver;
	
	@Before
	public void setUp() throws Exception {
		mapper = new CharCountMapper();
		reducer = new CharCountReducer();
		driver = new MapReduceDriver<LongWritable, Text, LineWritable, IntWritable, Text, IntWritable > (mapper, reducer);
	}

	@Test
	public final void testMain() {
		driver.withInput(new LongWritable(0), new Text("acba,\n"))
		.withInput(new LongWritable(0), new Text("Test"))
		.withOutput(new Text("1	,"), new IntWritable(1))
		.withOutput(new Text("1	a"), new IntWritable(2))
		.withOutput(new Text("1	b"), new IntWritable(1))
		.withOutput(new Text("1	c"), new IntWritable(1))
		.withOutput(new Text("2	T"), new IntWritable(1))
		.withOutput(new Text("2	e"), new IntWritable(1))
		.withOutput(new Text("2	s"), new IntWritable(1))
		.withOutput(new Text("2	t"), new IntWritable(1))		
		.runTest();
	}

}
