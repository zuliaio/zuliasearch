package io.zulia.server.analysis.filter;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class CaseProtectedWordsFilter extends KeywordMarkerFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final CharArraySet keywordSet;

    /**
     * Create a new CaseProtectedWordsFilter, that marks the current token as a
     * keyword if the tokens term buffer is contained in protected words
     *
     * @param in TokenStream to filter
     */
    public CaseProtectedWordsFilter(final TokenStream in) {
        super(in);
        this.keywordSet = new CharArraySet(1, false);
        this.keywordSet.add("AIDS");
    }

    @Override
    protected boolean isKeyword() {
        return keywordSet.contains(termAtt.buffer(), 0, termAtt.length());
    }

}
