package com.lifexweb.app.hadoop.CharCountPractice.util;

public class SurrogatePairUtil {

	public static int[] toCodePointArray(String str) {
		int length = str.length();
	    int[] codePointArray = new int[str.codePointCount(0, length)];
	    int pos = 0;         

	    for (int i = 0, codePoint; i < length; i += Character.charCount(codePoint)) {
	        codePoint = str.codePointAt(i);
	        codePointArray[pos++] = codePoint;
	    }
	    return codePointArray;
	}
	
}
