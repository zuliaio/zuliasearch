package io.zulia.rest.dto;

public class TermDTO {

	private String term;
	private long docFreq;
	private long termFreq;

	private double score;

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public long getDocFreq() {
		return docFreq;
	}

	public void setDocFreq(long docFreq) {
		this.docFreq = docFreq;
	}

	public long getTermFreq() {
		return termFreq;
	}

	public void setTermFreq(long termFreq) {
		this.termFreq = termFreq;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public String toString() {
		return "TermDTO{" + "term='" + term + '\'' + ", docFreq=" + docFreq + ", termFreq=" + termFreq + ", score=" + score + '}';
	}
}
