package io.zulia.data.source.spreadsheet.excel;

import io.zulia.data.common.HeaderMapping;

public record SheetInfo(int numberOfColumns, int numberOfRowsForSheet, HeaderMapping headerMapping) {
}