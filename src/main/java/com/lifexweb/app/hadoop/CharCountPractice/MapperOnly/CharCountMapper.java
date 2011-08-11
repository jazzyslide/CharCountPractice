package com.lifexweb.app.hadoop.CharCountPractice.MapperOnly;

import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lifexweb.app.hadoop.CharCountPractice.util.SurrogatePairUtil;

public class CharCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();
	private List<String> letterList = new ArrayList<String>();
	private String tmpStr = new String();
	
	@Override
	protected void map(LongWritable offset, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//文字列の正規化
		line = Normalizer.normalize(line, Normalizer.Form.NFC);
		//空白文字を削除
		line = line.replaceAll("\\s", "");
		int[] codePoints = SurrogatePairUtil.toCodePointArray(line);
		int fileCode = context.getTaskAttemptID().getTaskID().getId();
		keyText.set(fileCode + "\t" + offset);
		letterList.clear();
		
		//文字の含まれたリストを作成。ソート（昇順）をかける
		for (int codePoint : codePoints) {
			letterList.add(String.valueOf(Character.toChars(codePoint)));
		}
		Collections.sort(letterList);
		
		int count = 0;
		int loopCount = 0;
		tmpStr = "";
		for (String letter : letterList) {
			loopCount++;			
			if (tmpStr.isEmpty()) {
				tmpStr = letter;
				count++;			
			} else if (!tmpStr.equals(letter)){
				valueText.set(tmpStr + "\t" + count);
				context.write(keyText, valueText);
				tmpStr = letter;
				count = 1;
			} else {
				count++;
			}
			if (loopCount == letterList.size()) {
				valueText.set(tmpStr + "\t" + count);
				context.write(keyText, valueText);
			}
		}
	}
}
