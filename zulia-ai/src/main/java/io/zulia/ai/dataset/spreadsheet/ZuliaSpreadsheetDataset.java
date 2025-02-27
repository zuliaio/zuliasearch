package io.zulia.ai.dataset.spreadsheet;

import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.training.dataset.RandomAccessDataset;
import ai.djl.training.dataset.Record;
import ai.djl.util.Progress;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;
import io.zulia.data.source.spreadsheet.SpreadsheetSource;
import io.zulia.data.source.spreadsheet.SpreadsheetSourceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class ZuliaSpreadsheetDataset extends RandomAccessDataset {
	
	private final List<SpreadsheetRecord> recordList;
	private final Function<SpreadsheetRecordWithNDManager, NDList> featureCreator;
	private final Function<SpreadsheetRecordWithNDManager, NDList> labelCreator;
	
	public ZuliaSpreadsheetDataset(Builder builder) {
		super(builder);
		this.recordList = builder.recordList;
		this.featureCreator = builder.featureCreator;
		this.labelCreator = builder.labelCreator;
	}
	
	@Override
	public Record get(NDManager manager, long index) {
		SpreadsheetRecord record = recordList.get((int) index);
		SpreadsheetRecordWithNDManager spreadsheetRecordWithNDManager = new SpreadsheetRecordWithNDManager(record, manager);
		NDList featureList = featureCreator.apply(spreadsheetRecordWithNDManager);
		NDList labelList = labelCreator.apply(spreadsheetRecordWithNDManager);
		return new Record(featureList, labelList);
	}
	
	@Override
	protected long availableSize() {
		return recordList.size();
	}
	
	@Override
	public void prepare(Progress progress) {
	
	}
	
	public void shuffle() {
		Collections.shuffle(recordList);
	}
	
	public static final class Builder extends BaseBuilder<Builder> {
		private List<SpreadsheetRecord> recordList;
		private String filename;
		private Function<SpreadsheetRecordWithNDManager, NDList> featureCreator;
		private Function<SpreadsheetRecordWithNDManager, NDList> labelCreator;
		
		public String getFilename() {
			return filename;
		}
		
		public Builder setFilename(String filename) {
			this.filename = filename;
			return this;
		}
		
		public Function<SpreadsheetRecordWithNDManager, NDList> getFeatureCreator() {
			return featureCreator;
		}
		
		public Builder setFeatureCreator(Function<SpreadsheetRecordWithNDManager, NDList> featureCreator) {
			this.featureCreator = featureCreator;
			return this;
		}
		
		public Function<SpreadsheetRecordWithNDManager, NDList> getLabelCreator() {
			return labelCreator;
		}
		
		public Builder setLabelCreator(Function<SpreadsheetRecordWithNDManager, NDList> labelCreator) {
			this.labelCreator = labelCreator;
			return this;
		}
		
		@Override
		protected Builder self() {
			return this;
		}
		
		public ZuliaSpreadsheetDataset build() throws IOException {
			this.recordList = new ArrayList<>();
			try (SpreadsheetSource<?> source = SpreadsheetSourceFactory.fromFileWithHeaders(filename)) {
				
				for (SpreadsheetRecord record : source) {
					recordList.add(record);
				}
			}
			
			return new ZuliaSpreadsheetDataset(this);
		}
		
	}
}
