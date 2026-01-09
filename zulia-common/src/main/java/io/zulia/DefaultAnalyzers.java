package io.zulia;

import java.util.Set;

/**
 * Created by mdavis on 4/24/16.
 */
public class DefaultAnalyzers {

	public static final String KEYWORD = "keyword";
	public static final String LC_KEYWORD = "lcKeyword";
	public static final String LC_CONCAT_ALL = "lcConcatAll";
	public static final String STANDARD = "standard";
	public static final String STANDARD_HTML = "standardHtml";
	public static final String MIN_STEM = "minStem";
	public static final String KSTEMMED = "kstem";
	public static final String LSH = "lsh";
	public static final String TWO_TWO_SHINGLE = "twoTwoShingle";
	public static final String THREE_THREE_SHINGLE = "threeThreeShingle";

	public static Set<String> ALL_ANALYZERS = Set.of(KEYWORD, LC_KEYWORD, LC_CONCAT_ALL, STANDARD, STANDARD_HTML, MIN_STEM, KSTEMMED, LSH, TWO_TWO_SHINGLE,
			THREE_THREE_SHINGLE);
}
