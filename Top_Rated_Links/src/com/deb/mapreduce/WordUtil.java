package com.deb.mapreduce;

public class WordUtil {

	public static String cleanWords(String originalWord) {
		StringBuffer sb = new StringBuffer();
		for (char eachChar : originalWord.toCharArray())
			if (!isBadChar(eachChar)) {
				sb.append(eachChar);
			}

		return sb.toString();
	}

	public static boolean isBadChar(char eachChar) {
		if (eachChar == ',' || eachChar == ' ') {
			return true;
		}

		return false;
	}

}
