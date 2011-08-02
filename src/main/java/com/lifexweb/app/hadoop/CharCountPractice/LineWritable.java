package com.lifexweb.app.hadoop.CharCountPractice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LineWritable implements WritableComparable<LineWritable> {

	private IntWritable fileCode;
	private LongWritable offset;
	private IntWritable codePoint;
	
	public LineWritable() {
		set(new IntWritable(), new LongWritable(), new IntWritable());
	}
	
	public LineWritable(int fileCode, long offset, int codePoint) {
		set(new IntWritable(fileCode), new LongWritable(offset), new IntWritable(codePoint));
	}
	
	public LineWritable(IntWritable fileCode, LongWritable offset, IntWritable codePoint) {
		set(fileCode, offset, codePoint);
	}
	
	public void set(IntWritable fileCode, LongWritable offset, IntWritable codePoint) {
		this.fileCode = fileCode;
		this.offset = offset;
		this.codePoint = codePoint;
	}
	
	public IntWritable getFileCode() {
		return fileCode;
	}
	
	public LongWritable getOffset() {
		return offset;
	}

	public IntWritable getCodePoint() {
		return codePoint;
	}

	@Override
	public String toString() {
		return fileCode.toString() + "\t" + offset.get() + "\t" + codePoint.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		fileCode.readFields(in);
		offset.readFields(in);
		codePoint.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		fileCode.write(out);
		offset.write(out);
		codePoint.write(out);
	}
	
	public int hashCode() {
		return fileCode.hashCode() * 263 + offset.hashCode() * 163 + codePoint.hashCode();
	}

	public int compareTo(LineWritable cmpLine) {
		int cmp1 = fileCode.compareTo(cmpLine.fileCode);
		if (cmp1 != 0) {
			return cmp1;
		} else {
			int cmp2 = offset.compareTo(cmpLine.offset);
			if (cmp2 != 0) {
				return cmp2;
			}
			return codePoint.compareTo(cmpLine.codePoint);
		}
	}

}
