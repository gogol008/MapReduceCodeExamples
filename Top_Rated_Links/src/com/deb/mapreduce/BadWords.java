package com.deb.mapreduce;

public class BadWords {
	private static String[] badWords = { "abysmal", "adverse", "alarming", "angry", "annoy", "anxious", "apathy", "appalling", "atrocious", "awful", "bad", "banal", "barbed", "belligerent", "bemoan", "beneath", "boring", "broken"

	};

	public static boolean isBad(String sentence) {
		for (String eachBadWord : badWords) {
			if (sentence.contains(eachBadWord)) {
				return true;
			}

		}
		return false;
	}
	
	
}
