package com.lifexweb.app.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CharCountReducerTest {
	
	private CharCountReducer reducer;
	private ReduceDriver<LineWritable, IntWritable, Text, IntWritable> reducerDriver;
	private static final IntWritable ONE = new IntWritable(1);

	@Before
	public void setUp() throws Exception {
		reducer = new CharCountReducer();
		reducerDriver = new ReduceDriver<LineWritable, IntWritable, Text, IntWritable> (reducer);
	}

	@Test
	public final void testReduce() {
		reducerDriver.withInput(new LineWritable(0, "T"), Arrays.asList(ONE,ONE,ONE))
		.withOutput(new Text("1	T"), new IntWritable(3))
		.runTest();
		reducerDriver.resetOutput();

		reducerDriver.withInput(new LineWritable(1, ""), Arrays.asList(ONE,ONE))
		.runTest();
		reducerDriver.resetOutput();
		
		reducerDriver.withInput(new LineWritable(2, "t"), Arrays.asList(ONE,ONE))
		.withOutput(new Text("3	t"), new IntWritable(2))
		.runTest();
		reducerDriver.resetOutput();
	}

}
