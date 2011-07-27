package com.lifexweb.app.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LineWritable implements WritableComparable<LineWritable> {

	private IntWritable fileCode;
	private LongWritable offset;
	private Text text;
	
	public LineWritable() {
		set(new IntWritable(), new LongWritable(), new Text());
	}
	
	public LineWritable(int fileCode, long offset, String text) {
		set(new IntWritable(fileCode), new LongWritable(offset), new Text(text));
	}
	
	public LineWritable(IntWritable fileCode, LongWritable offset, Text text) {
		set(fileCode, offset, text);
	}
	
	public void set(IntWritable fileCode, LongWritable offset, Text text) {
		this.fileCode = fileCode;
		this.offset = offset;
		this.text = text;
	}
	
	public IntWritable getFileCode() {
		return fileCode;
	}
	
	public LongWritable getOffset() {
		return offset;
	}

	public Text getText() {
		return text;
	}

	@Override
	public String toString() {
		return fileCode.toString() + "\t" + offset.get() + "\t" + text.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		fileCode.readFields(in);
		offset.readFields(in);
		text.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		fileCode.write(out);
		offset.write(out);
		text.write(out);
	}
	
	public int hashCode() {
		return fileCode.hashCode() * 263 + offset.hashCode() * 163 + text.hashCode();
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
			return text.compareTo(cmpLine.text);
		}
	}

}
