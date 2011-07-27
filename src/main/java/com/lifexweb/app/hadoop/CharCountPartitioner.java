package com.lifexweb.app.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CharCountPartitioner extends Partitioner<LineWritable, IntWritable> {	
	
	@Override
	public int getPartition(LineWritable keyLine, IntWritable count, int numReducer) {
		return ((keyLine.getFileCode().hashCode() + keyLine.getOffset().hashCode()) & Integer.MAX_VALUE) % numReducer;
	}

}
