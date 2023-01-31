package io.zulia.server.analysis.frequency;

import com.google.common.collect.Ordering;
import io.zulia.message.ZuliaBase.Term;
import io.zulia.message.ZuliaQuery.AnalysisRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * Created by mdavis on 6/28/16.
 */
public class TermFreq {

	private final DocFreq docFreq;
	private final HashMap<String, Term.Builder> tokenCount;
	private List<Term.Builder> terms;

	public TermFreq(DocFreq docFreq) {
		this.docFreq = docFreq;
		this.tokenCount = new HashMap<>();
	}

	public void addTerm(String term) throws IOException {

		Term.Builder lmTerm = tokenCount.get(term);
		if (lmTerm == null) {
			lmTerm = Term.newBuilder().setTermFreq(0).setValue(term);
			tokenCount.put(term, lmTerm);
			if (docFreq != null) {
				int docFreq = this.docFreq.getDocFreq(term);
				lmTerm.setDocFreq(docFreq);
			}
		}

		lmTerm.setTermFreq(lmTerm.getTermFreq() + 1);
	}

	public void addTerm(Term.Builder term) {
		Term.Builder lmTerm = tokenCount.get(term.getValue());
		if (lmTerm == null) {
			lmTerm = Term.newBuilder(term.buildPartial());
			tokenCount.put(term.getValue(), lmTerm);
		}
		else {
			lmTerm.setTermFreq(lmTerm.getTermFreq() + term.getTermFreq());
		}
	}

	public List<Term.Builder> getTopTerms(int topN, AnalysisRequest.TermSort termSort) {

		if (terms == null) {
			terms = new ArrayList<>(tokenCount.values());
		}

		if (AnalysisRequest.TermSort.TFIDF.equals(termSort)) {
			if (docFreq != null) {
				for (Term.Builder term : terms) {
					double score = docFreq.getScoreForTerm(term.getTermFreq(), term.getDocFreq());
					term.setScore(score);
				}
			}
		}

		return getTopTerms(terms, topN, termSort);

	}

	public static List<Term.Builder> getTopTerms(List<Term.Builder> terms, int n, AnalysisRequest.TermSort termSort) {
		Comparator<Term.Builder> ordering = (Term.Builder o1, Term.Builder o2) -> {
			if (AnalysisRequest.TermSort.TF.equals(termSort)) {
				return Long.compare(o1.getTermFreq(), o2.getTermFreq());
			}
			else if (AnalysisRequest.TermSort.TFIDF.equals(termSort)) {
				return Double.compare(o1.getScore(), o2.getScore());
			}
			else if (AnalysisRequest.TermSort.ABC.equals(termSort)) {
				return o2.getValue().compareTo(o1.getValue());
			}
			else {
				return 0;
			}
		};

		if (n != 0) {
			//seems to be the most efficient according to
			//http://www.michaelpollmeier.com/selecting-top-k-items-from-a-list-efficiently-in-java-groovy/
			return Ordering.from(ordering).greatestOf(terms, n);
		}
		else {
			terms.sort(ordering.reversed());
			return terms;
		}
	}

}
