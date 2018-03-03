package io.zulia.server.index;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.FuzzyTermsEnum;
import org.apache.lucene.search.MaxNonCompetitiveBoostAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class ShardTermsHandler {

	private final DirectoryReader indexReader;

	public ShardTermsHandler(DirectoryReader indexReader) {
		this.indexReader = indexReader;
	}

	public GetTermsResponse handleShardTerms(GetTermsRequest request) throws IOException {
		GetTermsResponse.Builder builder = GetTermsResponse.newBuilder();

		String fieldName = request.getFieldName();

		SortedMap<String, ZuliaBase.Term.Builder> termsMap = new TreeMap<>();

		if (request.getIncludeTermCount() > 0) {

			Set<String> includeTerms = new TreeSet<>(request.getIncludeTermList());
			List<BytesRef> termBytesList = new ArrayList<>();
			for (String term : includeTerms) {
				BytesRef termBytes = new BytesRef(term);
				termBytesList.add(termBytes);
			}

			for (LeafReaderContext subReaderContext : indexReader.leaves()) {
				Terms terms = subReaderContext.reader().terms(fieldName);

				if (terms != null) {

					TermsEnum termsEnum = terms.iterator();
					for (BytesRef termBytes : termBytesList) {
						if (termsEnum.seekExact(termBytes)) {
							BytesRef text = termsEnum.term();
							handleTerm(termsMap, termsEnum, text, null, null);
						}

					}
				}

			}
		}
		else {

			AttributeSource atts = null;
			MaxNonCompetitiveBoostAttribute maxBoostAtt = null;
			boolean hasFuzzyTerm = request.hasFuzzyTerm();
			if (hasFuzzyTerm) {
				atts = new AttributeSource();
				maxBoostAtt = atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
			}

			BytesRef startTermBytes;
			BytesRef endTermBytes = null;

			if (!request.getStartTerm().isEmpty()) {
				startTermBytes = new BytesRef(request.getStartTerm());
			}
			else {
				startTermBytes = new BytesRef("");
			}

			if (!request.getEndTerm().isEmpty()) {
				endTermBytes = new BytesRef(request.getEndTerm());
			}

			Pattern termFilter = null;
			if (!request.getTermFilter().isEmpty()) {
				termFilter = Pattern.compile(request.getTermFilter());
			}

			Pattern termMatch = null;
			if (!request.getTermMatch().isEmpty()) {
				termMatch = Pattern.compile(request.getTermMatch());
			}

			for (LeafReaderContext subReaderContext : indexReader.leaves()) {
				Terms terms = subReaderContext.reader().terms(fieldName);

				if (terms != null) {

					if (hasFuzzyTerm) {
						ZuliaBase.FuzzyTerm fuzzyTerm = request.getFuzzyTerm();
						FuzzyTermsEnum termsEnum = new FuzzyTermsEnum(terms, atts, new Term(fieldName, fuzzyTerm.getTerm()), fuzzyTerm.getEditDistance(),
								fuzzyTerm.getPrefixLength(), !fuzzyTerm.getNoTranspositions());
						BytesRef text = termsEnum.term();

						handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

						while ((text = termsEnum.next()) != null) {
							handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
						}

					}
					else {
						TermsEnum termsEnum = terms.iterator();
						TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(startTermBytes);

						if (!seekStatus.equals(TermsEnum.SeekStatus.END)) {
							BytesRef text = termsEnum.term();

							if (endTermBytes == null || (text.compareTo(endTermBytes) < 0)) {
								handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

								while ((text = termsEnum.next()) != null) {

									if (endTermBytes == null || (text.compareTo(endTermBytes) < 0)) {
										handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
									}
									else {
										break;
									}
								}
							}
						}
					}

				}

			}
		}

		for (ZuliaBase.Term.Builder termBuilder : termsMap.values()) {
			builder.addTerm(termBuilder.build());
		}

		return builder.build();
	}

	private void handleTerm(SortedMap<String, ZuliaBase.Term.Builder> termsMap, TermsEnum termsEnum, BytesRef text, Pattern termFilter, Pattern termMatch)
			throws IOException {

		String textStr = text.utf8ToString();
		if (termFilter != null || termMatch != null) {

			if (termFilter != null) {
				if (termFilter.matcher(textStr).matches()) {
					return;
				}
			}

			if (termMatch != null) {
				if (!termMatch.matcher(textStr).matches()) {
					return;
				}
			}
		}

		if (!termsMap.containsKey(textStr)) {
			termsMap.put(textStr, ZuliaBase.Term.newBuilder().setValue(textStr).setDocFreq(0).setTermFreq(0));
		}
		ZuliaBase.Term.Builder builder = termsMap.get(textStr);
		builder.setDocFreq(builder.getDocFreq() + termsEnum.docFreq());
		builder.setTermFreq(builder.getTermFreq() + termsEnum.totalTermFreq());
		BoostAttribute boostAttribute = termsEnum.attributes().getAttribute(BoostAttribute.class);
		if (boostAttribute != null) {
			builder.setScore(boostAttribute.getBoost());
		}

	}
}
