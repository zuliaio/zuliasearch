package io.zulia.util.schema;

import java.util.Date;

public class FieldStats {

	private String key;

	private int collection;
	private int document;
	private int number;
	private int string;
	private int date;

	public FieldStats(String key) {
		this.key = key;
	}

	public void tallyType(Object o) {
		if (o instanceof Number) {
			number++;
		}
		else if (o instanceof String) {
			string++;
		}
		else if (o instanceof Date) {
			date++;
		}

	}

	public void tallyCollection() {
		collection++;
	}

	public void tallyDocument() {
		document++;
	}

	public int getCollection() {
		return collection;
	}

	public int getDocument() {
		return document;
	}

	public int getNumber() {
		return number;
	}

	public int getString() {
		return string;
	}

	public int getDate() {
		return date;
	}

	public void incrementByCounts(FieldStats fieldStats) {
		this.date += fieldStats.getDate();
		this.string += fieldStats.getString();
		this.document += fieldStats.getDocument();
		this.number += fieldStats.getNumber();
		this.collection += fieldStats.getCollection();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (document > 0) {
			sb.append(" document(").append(document).append(") ");
		}
		if (collection > 0) {
			sb.append(" collection(").append(collection).append(") ");
		}
		if (number > 0) {
			sb.append(" number(").append(number).append(") ");
		}
		if (string > 0) {
			sb.append(" string(").append(string).append(") ");
		}
		if (date > 0) {
			sb.append(" date(").append(date).append(") ");
		}

		return sb.toString().trim();
	}

	public void incrementByIfExists(FieldStats fieldStats) {
		if (fieldStats.getDate() > 0) {
			this.date++;
		}
		if (fieldStats.getString() > 0) {
			this.string++;
		}
		if (fieldStats.getDocument() > 0) {
			this.document++;
		}
		if (fieldStats.getNumber() > 0) {
			this.number++;
		}
		if (fieldStats.getCollection() > 0) {
			this.collection++;
		}
	}

	public String displayDocStats(int totalDocs) {
		StringBuilder sb = new StringBuilder();
		if (document > 0) {
			sb.append(" document(").append(formatPercent(document, totalDocs)).append(") ");
		}
		if (collection > 0) {
			sb.append(" collection(").append(formatPercent(collection, totalDocs)).append(") ");
		}
		if (number > 0) {
			sb.append(" number(").append(formatPercent(number, totalDocs)).append(") ");
		}
		if (string > 0) {
			sb.append(" string(").append(formatPercent(string, totalDocs)).append(") ");
		}
		if (date > 0) {
			sb.append(" date(").append(formatPercent(date, totalDocs)).append(") ");
		}

		return sb.toString().trim();
	}

	public String displayFieldPerDocStats(int totalDocs) {
		StringBuilder sb = new StringBuilder();
		if (document > 0) {
			sb.append(" document(").append(formatPerDoc(document, totalDocs)).append(") ");
		}
		if (collection > 0) {
			sb.append(" collection(").append(formatPerDoc(collection, totalDocs)).append(") ");
		}
		if (number > 0) {
			sb.append(" number(").append(formatPerDoc(number, totalDocs)).append(") ");
		}
		if (string > 0) {
			sb.append(" string(").append(formatPerDoc(string, totalDocs)).append(") ");
		}
		if (date > 0) {
			sb.append(" date(").append(formatPerDoc(date, totalDocs)).append(") ");
		}

		return sb.toString().trim();
	}

	public String formatPercent(int count, int total) {
		return String.format("%.3f", ((count * 100.0) / total)) + "%";
	}

	public String formatPerDoc(int count, int total) {
		return String.format("%.2f", (((double) count) / total)) + "/doc";
	}
}