package io.zulia.ai.dataset.spreadsheet;

import ai.djl.ndarray.NDManager;
import io.zulia.data.source.spreadsheet.SpreadsheetRecord;

public record SpreadsheetRecordWithNDManager(SpreadsheetRecord record, NDManager ndManager) {
}
