package com.lifexweb.app.hadoop;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CharCountReducerTest {
	
	private CharCountReducer reducer;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reducerDriver;
	private static final IntWritable ONE = new IntWritable(1);

	@Before
	public void setUp() throws Exception {
		reducer = new CharCountReducer();
		reducerDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable> (reducer);
	}

	@Test
	public final void testReduce() {
		reducerDriver.withInput(new Text("1	T"), Arrays.asList(ONE,ONE,ONE))
		.withOutput(new Text("1	T"), new IntWritable(3))
		.runTest();
		
		reducerDriver.resetOutput();
		
		reducerDriver.withInput(new Text("2	T"), Arrays.asList(ONE,ONE))
		.withOutput(new Text("2	T"), new IntWritable(2))
		.runTest();
	}

}
