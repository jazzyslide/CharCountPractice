package com.lifexweb.app.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class LineWritable implements WritableComparable<LineWritable> {

	private LongWritable offset;
	private Text text;
	
	public LineWritable() {
		set(new LongWritable(), new Text());
	}
	
	public LineWritable(long offset, String text) {
		set(new LongWritable(offset), new Text(text));
	}
	
	public LineWritable(LongWritable offset, Text text) {
		set(offset, text);
	}
	
	public void set(LongWritable offset, Text text) {
		this.offset = offset;
		this.text = text;
	}
	
	public LongWritable getOffset() {
		return offset;
	}

	public Text getText() {
		return text;
	}

	@Override
	public String toString() {
		return offset.get() + "\t" + text.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		offset.readFields(in);
		text.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		offset.write(out);
		text.write(out);
	}

	public int compareTo(LineWritable cmpLine) {
		int cmp = offset.compareTo(cmpLine.offset);
		if (cmp != 0) {
			return cmp;
		}
		return text.compareTo(cmpLine.text);
	}

}
